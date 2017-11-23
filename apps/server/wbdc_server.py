# wbdc_server.py
from __future__ import print_function
import logging
import time
import functools
import json
import random
import datetime
import os

import Pyro4

from support.threading_util import PausableThread, iterativeRun
try:
    from support.pyro import Pyro4Server, get_device_server, Pyro4ServerError
except ImportError as err:
    from pyro_support import Pyro4Server
from MonitorControl.Configurations.CDSCC import FO_patching
from DSS43Backend.automation import AutoTestAnnotation

Pyro4.config.COMMTIMEOUT = 0.0

module_logger = logging.getLogger()

class WBDCFEPublisherThread(PausableThread):
    """
    A thread that publishes power meter readings from the radiometer.
    """

    def __init__(self, wbdc_fe_server, thread_name, bus=None):
        PausableThread.__init__(self, name=thread_name)

        self.wbdc_fe_server = wbdc_fe_server
        self.bus = bus

    @iterativeRun
    def run(self):
        readings = self.wbdc_fe_server.get_tsys()
        if self.bus:
            self.bus.send('power_meter', readings)

        with self.wbdc_fe_server._lock:
            self.wbdc_fe_server.pm_readings = readings


class ParserDecorator(object):
    """
    Decorator for parsing method arguments that can be either string or bool.
    Pass the desired string to bool correspondence,
    and it will return the corresponding set_WBDC opt

    Right now, this decorator will only work with method calls, not regular functions.
    """

    def __init__(self, str_correspondance, opt_correspondance, mode=None, returns=None):
        """

        Args:
            str_correspondance (list, tuple):
            opt_correspondance (list, tuple):
        """
        self.str_corr = str_correspondance
        self.opt_corr = opt_correspondance
        self.mode = mode
        self.annotation_obj = AutoTestAnnotation(args=(self.str_corr[0], ), returns=returns)

    def __call__(self, f):

        func = self.annotation_obj(f)

        @functools.wraps(func)
        def wrapper(obj, state):
            try:
                if isinstance(state, str):
                    if state.lower().strip() not in self.str_corr:
                        raise ValueError("Argument {} not recognized".format(state))
                elif isinstance(state, bool):
                    if state:
                        state = self.str_corr[0]
                    else:
                        state = self.str_corr[1]

                if state.lower().strip() == self.str_corr[0]:
                    opt = self.opt_corr[0]
                    state = self.str_corr[0]
                elif state.lower().strip() == self.str_corr[1]:
                    opt = self.opt_corr[1]
                    state = self.str_corr[1]

                if self.mode:
                    setattr(obj, self.mode, state)

                return func(obj, opt)

            except Exception as err:
                error_msg = "Error parsing argument {} of type {}: {}".format(state, type(state), err)
                raise ValueError(error_msg)

        return wrapper


def error_decorator(fn):
    """
    A convenience decorator to handle errors in get/set methods.
    Set up to work with method calls, not regular function calls.
    Args:
        fn (function): The wrapped function
    Returns:
        function: The wrapper
    """

    @functools.wraps(fn)
    def wrapper(obj, *args):
        try:
            return fn(obj, *args)
        except Exception as err:
            error_msg = "Error in {}: {}".format(fn.__name__, err)
            obj.serverlog.error(error_msg, exc_info=True)
            raise Pyro4ServerError(error_msg)

    return wrapper


@Pyro4.expose
class WBDCFrontEndServer(Pyro4Server):
    """
    A class for integrating FrontEnd and WBDC control.
    In principle the FrontEnd and WBDC servers should be separate interfaces, but in the
    case of CDSCC control is integrated together.

    """
    def __init__(self, name="WBDC_FE",
                 settings_file="/home/ops/roach_data/sao_test_data/log_dir/WBDCFrontEndsettings.json",
                 patching_file_path=None, logfile=None, loglevel=logging.INFO, **kwargs):

        logger = kwargs.get('logger', logging.getLogger(__name__+".WBDCFrontEndServer"))
        # Pyro4Server.__init__(self, name, logger=logger, logfile=logfile, loglevel=loglevel, **kwargs)
        Pyro4Server.__init__(self, name, logfile=logfile, logger=logger, **kwargs)
        if not patching_file_path:
            self._dist_assmbly = FO_patching.DistributionAssembly()
        else:
            self._dist_assmbly = FO_patching.DistributionAssembly(parampath=patching_file_path)

        # Both connect_to_hardware and simulate set attributes.
        if not self._simulated:
            self.connect_to_hardware()
        elif self._simulated:
            self.simulate()

        self.settings_file = settings_file
        # Set the distribution assembly
        self._pm_patching_inputs = self.get_pm_patching_sources()
        self.serverlog.debug(self._pm_patching_inputs)
        self._pm_att = self.get_pm_atten_names(self._pm_patching_inputs)
        self.serverlog.debug(self._pm_att)

        # Set the internal Power Meter states.
        # These values get updated if we call corresponding get/set methods
        self._PM_mode = 'W'
        self._polarizer_mode = 'circular'
        self._IF_hybrid_mode = 'iq'

        # initialize tsysfactor* attributes
        self.tsysfactor1 = 1.0
        self.tsysfactor2 = 1.0
        self.tsysfactor3 = 1.0
        self.tsysfactor4 = 1.0

        # We don't call self.get_WBDCFrontEnd_state at instantiation
        # because it takes half a century to run (on the order of twenty seconds)
        self.WBDCFrontEnd_summary = None
        if not os.path.isfile(self.settings_file):
            open(self.settings_file, 'w').close() # make sure file exists

        # attempt to get old minical calibration data. (This updates the tsys values)
        self.retrieve_previous_minical_results()

        # Set the 'modes' of the WBDC:
        self._modes = ['A2LiP1I', 'A2LiP1Q', 'A2LiP2I', 'A2LiP2Q',
                      'A2LiP1L', 'A2LiP1U', 'A2LiP2L', 'A2LiP2U',
                      'A2CiP1I', 'A2CiP1Q', 'A2CiP2I', 'A2CiP2Q',
                      'A2CiP1L', 'A2CiP1U', 'A2CiP2L', 'A2CiP2U',
                      'A1LiP1I', 'A1LiP1Q', 'A1LiP2I', 'A1LiP2Q',
                      'A1LiP1L', 'A1LiP1U', 'A1LiP2L', 'A1LiP2U',
                      'A1CiP1I', 'A1CiP1Q', 'A1CiP2I', 'A1CiP2Q',
                      'A1CiP1L', 'A1CiP1U', 'A1CiP2L', 'A1CiP2U',
                      'A1Load', 'A2Load']

    def connect_to_hardware(self, *args):
        """
        Connect to WBDC and FrontEnd hardware servers.
        Returns:
            None
        """
        # WBDC server
        self.wbdc = get_device_server('wbdc2hw_server-dss43wbdc2', pyro_ns="crux")
        # FE server
        self.FE = get_device_server('FE_server-krx43', pyro_ns="crux")
        self.serverlog.debug("Successfully got Pyro3 objects")
        self._simulated = False

    def simulate(self):
        """
        Turn on simulation mode.
        Returns:
            None
        """
        self.serverlog.debug("Entering simulation mode.")
        self.simulated_atten = {'R1-18-E': [15, 'dB'], 'R1-18-H': [15, 'dB'],
                                'R1-20-E': [15, 'dB'], 'R1-20-H': [10, 'dB'],
                                'R1-22-E': [15, 'dB'], 'R1-22-H': [15, 'dB'],
                                'R1-24-E': [15, 'dB'], 'R1-24-H': [None, 'dB'],
                                'R1-26-E': [10, 'dB'], 'R1-26-H': [15, 'dB'],
                                'R2-18-E': [15, 'dB'], 'R2-18-H': [15, 'dB'],
                                'R2-20-E': [15, 'dB'], 'R2-20-H': [15, 'dB'],
                                'R2-22-E': [15, 'dB'], 'R2-22-H': [5, 'dB'],
                                'R2-24-E': [15, 'dB'], 'R2-24-H': [15, 'dB'],
                                'R2-26-E': [15, 'dB'], 'R2-26-H': [15, 'dB']}

        self.simulated_feed_state = {1: 'sky', 2: 'sky'}
        self.simulated_crossover_switch_state = {1: False, 2: False}
        self.simulated_noise_diode_state = False
        self.simulated_preamp_bias = {1: False, 2: False}
        self._simulated = True

    def get_tsys_factors(self):
        """
        Get the tsys factors that allow for conversion between raw power meter output
        and sky temperature
        Returns:
            dict: Keys are power meter names ('PM1', etc), values are tsys factors
        """
        return {'PM1': self.tsysfactor1,
                'PM2': self.tsysfactor2,
                'PM3': self.tsysfactor3,
                'PM4': self.tsysfactor4}

    @property
    def modes(self):
        return self._modes

    @property
    def PM_mode(self):
        return self._PM_mode

    @property
    def polarizer_mode(self):
        return self._polarizer_mode

    @property
    def IF_hybrid_mode(self):
        return self._IF_hybrid_mode

    @property
    def pm_att(self):
        return self._pm_att

    def _set_WBDC(self, opt):
        """
        Notes:

        A descriptive string is one that is highly human readable.
        Tells us the status of the requested component, plus date and time
        information. For example, self._set_WBDC(23) will give the following response:
        ```
        >> server._set_WBDC(23)
        'Noise diode turned on at Wed Mar 15 14:39:38 2017'
        ```
        Option:

        12 - FE   - check load states: descriptive str
        13 - FE   - feed 1 to sky: bool (False)
        14 - FE   - feed 1 to load: bool (True)
        15 - FE   - feed 2 to sky: bool (False)
        16 - FE   - feed 2 to load: bool (True)
        18 - FE   - compute Y-factors with all four power meters: Not working as of 15/02/2017
        22 - FE   - report noise diode state: str, either 'on' or 'off'
        23 - FE   - set noise diode on: descriptive str
        24 - FE   - set noise diode off: descriptive str
        25 - FE   - feed 1 pre-amps bias on: descriptive str
        26 - FE   - feed 1 pre-amps bias off: descriptive str
        27 - FE   - feed 2 pre-amps bias on: descriptive str
        28 - FE   - feed 2 pre-amps bias off: descriptive str
        29 - FE   - do minicals with all four power meters:
        31 - FE   - report front end temperatures: str with Python dict
        32 - FE   - compute system temperatures from Y-factors (do 18 first!)
        38 - WBDC - report analog data: dict
        41 - WBDC - set crossover switch: True
        42 - WBDC - unset crossover switch: False
        43 - WBDC - set polarizers to circular
        44 - WBDC - set polarizers to linear
        45 - WBDC - set IF hybrids to IQ
        46 - WBDC - set IF hybrids to LU
        391 - FE   - set PM1 to W
        392 - FE   - set PM2 to W
        393 - FE   - set PM3 to W
        394 - FE   - set PM4 to W
        401 - FE   - set PM1 to dB
        402 - FE   - set PM2 to dB
        403 - FE   - set PM3 to dB
        404 - FE   - set PM4 to dB
        """
        self.serverlog.debug("_set_WBDC: called for {}".format(opt))
        if (opt > 11 and opt < 17) or (opt > 19 and opt < 30) or (opt > 30 and opt < 37):
            try:
                result = self.FE.set_WBDC(opt)
            except Exception, details:
                self.serverlog.error("_set_WBDC: failed because {}".format(details))
                result = "False"
            self.serverlog.debug("_set_WBDC: returned {}".format(result))
            return result
        elif opt == 18:
            # raise Pyro4ServerError("Can't calculate Y-factor right now.")
            # Yfactors, text = FElj.Y_factors(self.pm)
            text = self.FE.set_WBDC(opt)
            # text = ("Y-factors at " + time.ctime(time.time()) + "\n") + (str(4 * [0.0]) + "\n")
            return text
        elif opt == 38:
            return self.wbdc.set_WBDC(opt)
        elif opt > 390 and opt < 395:
            return self.FE.set_WBDC(opt)
        elif opt > 400 and opt < 405:
            return self.FE.set_WBDC(opt)
        elif opt > 40 and opt < 53:
            return self.wbdc.set_WBDC(opt)
        else:
            return "Invalid option", opt

    def get_WBDCFrontEnd_state(self, save_config=True):
        """
        Summary report of the receiver state.

        Returns a report with the time, the load positions, the cross-switch
        state, the states of the polarization hybrids, and the states of the IF
        hybrids. (It also returns dummy values for the K1 band switch and the
        local oscillator lock.)

        This function takes a long time to run (~20 seconds)
        """
        self.serverlog.debug("get_WBDCFrontEnd_state: called ")
        report = []
        report_dict = {}
        # time
        t = time.ctime(time.time())
        report.append(t)
        report_dict['time'] = t

        # load/sky state
        try:
            load_state = self.get_feed_state()
            report.append(load_state)
            report_dict['feed_state'] = load_state
        except Exception, details:
            self.serverlog.error("get_WBDCFrontEnd_state: getting load state failed because {}".format(details))
            report.append("Feed 1 on sky\nFeed 2 on sky\n")
            report_dict['feed_state'] = None

        # preamp bias (no get method yet)

        # noise diode
        try:
            noise_diode_state = self.get_noise_diode_state()
            report_dict['noise_diode_state'] = noise_diode_state
        except Exception, details:
            self.serverlog.error("get_WBDCFrontEnd_state: getting noise diode state failed: {}".format(details))
            report_dict['noise_diode_state'] = None

        # cross-switch state
        try:
            cross_switch_state = self.get_crossover_switch()
            report.append(cross_switch_state)
            report_dict['cross_switch_state'] = cross_switch_state
        except Exception, details:
            self.serverlog.error("get_WBDCFrontEnd_state: getting cross switch failed because {}".format(details))
            report_dict['cross_switch_state'] = None
        # polarization state
        try:
            polstates = self.get_polarizers()
            self.serverlog.debug("report_WBDC: polarization states: %s", str(polstates))
            pol_states_dict = {'22': [polstates["R1-22"], polstates["R2-22"]],
                               '20': [polstates["R1-20"], polstates["R2-20"]],
                               '18': [polstates["R1-18"], polstates["R2-18"]],
                               '24': [polstates["R1-24"], polstates["R2-24"]],
                               '26': [polstates["R1-26"], polstates["R2-26"]]}
            report.append(pol_states_dict)
            report_dict['polarizer_state'] = polstates
        except Exception, details:
            self.serverlog.error("get_WBDCFrontEnd_state: getting pol_sec failed because {}".format(details))
            report_dict['polarizer_state'] = None
        # band
        try:
            band_state = 24
            report.append(band_state)
            report_dict['band_state'] = band_state
        except Exception, details:
            self.serverlog.error("get_WBDCFrontEnd_state: getting LO freq. failed because {}".format(details))
            report_dict['band_state'] = None
        # LO lock
        try:
            lo_lock_state = [True for i in xrange(5)]
            report.append(lo_lock_state)
            report_dict['lo_lock_state'] = lo_lock_state
        except Exception, details:
            self.serverlog.error("get_WBDCFrontEnd_state: getting PLOs failed because {}".format(details))
            report_dict['lo_lock_state'] = None

        # IF hybrids state
        try:
            DCstates = self.get_IF_hybrids()
            self.serverlog.debug("get_WBDCFrontEnd_state: DC states: %s", DCstates)
            dc_states_dict = {0: {0: DCstates['R1-22P1'], 1: DCstates['R1-22P2']},
                              1: {0: DCstates['R2-22P1'], 1: DCstates['R2-22P2']}}
            report.append(dc_states_dict)
            report_dict['IF_hybrid_state'] = DCstates
        except Exception, details:
            self.serverlog.error("get_WBDCFrontEnd_state: getting IF hybrid state failed because {}".format(details))
            report_dict['IF_hybrid_state'] = None
        # Attenuator state
        try:
            attens = self.get_attens()
            report_dict['attens'] = attens.copy()
        except Exception, details:
            self.serverlog.error("get_WBDCFrontEnd_state: getting attenuations failed: {}".format(details))
            report_dict['attens'] = None

        self.serverlog.debug("get_WBDCFrontEnd_state:\n %s", str(report))
        self.WBDCFrontEnd_summary = report_dict.copy()
        if save_config:
            self.serverlog.debug("Saving current configuration to file {}".format(self.settings_file))
            with open(self.settings_file, 'r+') as f:
                try:
                    data = json.load(f)
                    data.update({'settings': report_dict})
                    f.seek(0)
                    json.dump(data, f)
                    f.truncate()
                except ValueError:
                    f.seek(0)
                    json.dump({'settings': report_dict}, f)
                    f.truncate()

        return report, report_dict

    def set_WBDCFrontEnd_state(self, config_file='default'):
        """
        Uses the internal WBDCFrontEnd_summary attribute to reset parameters.
        Returns:
            None
        """
        if self.WBDCFrontEnd_summary:
            summary = self.WBDCFrontEnd_summary.copy()
        else:
            self.serverlog.debug("Couldn't use internal summary attribute -- using config file.")
            if config_file == 'default':
                config_file = self.settings_file
            try:
                with open(config_file, 'r') as f:
                    summary = json.load(f)['settings']
            except Exception, err:
                self.serverlog.error("Couldn't load in configuration file: {}".format(err), exc_info=True)
                return
        # For resetting the polarizar state and IF_hybrids we assume that the dictionaries
        # resulting from the respective get methods will be the same.

        # reset the polarizers
        pol_state = summary['polarizer_state']
        pol_state = list(set(pol_state))
        if len(pol_state) == 1:
            if pol_state[0] == 0:
                self.set_polarizers('linear')
            elif pol_state[0] == 1:
                self.set_polarizers('circular')

        # reset the IF hybrids
        if_state = summary['IF_hybrid_state']
        if_state = list(set(if_state))
        if len(if_state) == 1:
            if if_state[0] == 0:
                self.set_IF_hybrids('lu')
            elif if_state[0] == 1:
                self.set_IF_hybrids('iq')

        # reset the attenuators
        attens = summary['attens']
        self.serverlog.debug(attens)
        for atten_name in attens:
            val = attens[atten_name]
            if val:
                self.set_atten(atten_name, val[0])

        # reset the feed state
        feed_state = summary['feed_state']  # ['sky', 'sky'] for example
        self.set_feed_state(1, feed_state[0])
        self.set_feed_state(2, feed_state[1])

        # reset the cross switch
        x_switch_state = summary['cross_switch_state']  # just bool value
        self.set_crossover_switch(x_switch_state)

        # preamp bias (not implemented yet)

        # noise diode
        noise_diode_state = summary['noise_diode_state']  # just bool value
        self.set_noise_diode_state(noise_diode_state)

    @AutoTestAnnotation()
    def get_pm_patching_sources(self):
        """
        report which receiver outputs feed the power meters
        """
        pm_inputs = self._dist_assmbly.get_signals("Power Meter")
        self.serverlog.debug("get_pm_patching_sources: pm_inputs: {}".format(pm_inputs))
        return pm_inputs

    @AutoTestAnnotation()
    def get_pm_atten_names(self, pm_patching_inputs=None):
        """
        Args:
            pm_inputs: The power meter inputs, the result of self.pm_patching_sources
        Returns:
            dict: attenuator names
        """
        if not pm_patching_inputs:
            pm_patching_inputs = self.get_pm_patching_sources()
        att = {}
        for key in pm_patching_inputs.keys():
            self.serverlog.debug("_attenuator_names: %s", pm_patching_inputs[key])
            attkey = int(key[-1])
            att[attkey] = 'R' + str(pm_patching_inputs[key]['Receiver'])
            att[attkey] += '-' + str(pm_patching_inputs[key]['Band'])
            att[attkey] += '-' + pm_patching_inputs[key]['Pol']
        self.serverlog.debug("get_pm_atten_names: PM attenuator names: {}".format(att))
        return att

    @AutoTestAnnotation()
    def get_atten_names(self):
        """
        Get the names of all the attenuators
        """
        if not self._simulated:
            return self.wbdc.get_atten_IDs()
        else:
            return self.simulated_atten.keys()

    @AutoTestAnnotation(args=('R1-24-E', ))
    def get_atten(self, atten_name):
        """
        Get PIN diode attenuator for specific IF channel
        Args:
            atten_name(str): attenutor name
        """
        # self.serverlog.debug("get_atten: attenuator name: {}".format(atten_name))
        if not self._simulated:
            return self.wbdc.get_atten(atten_name)
        else:
            return self.simulated_atten[atten_name][0]

    @AutoTestAnnotation()
    def get_attens(self):
        """
        Get the attenuator values for ALL the attenuators, including ones that are
        not associated with Power Meter heads.
        Returns:
            dict: keys are attenuator names, values are tuple with attenuations and 'dB'
        """
        if not self._simulated:
            report = {}
            for name in self.get_atten_names():
                report[name] = (self.wbdc.get_atten(name), 'dB')
            return report
        else:
            return self.simulated_atten.copy()

    @AutoTestAnnotation()
    def get_pm_attens(self):
        """
        Get the attenuator values associated with the power meters heads.
        Returns:
            dict: keys are attenuator names, values are tuple with attenuations and 'dB'
        """
        report = {}
        for name in self._pm_att:
            atten_name = self._pm_att[name]
            if not self._simulated:
                report[name] = (self.wbdc.get_atten(atten_name), self.PM_mode)
            else:
                report[name] = self.simulated_atten[atten_name]
        return report

    @AutoTestAnnotation()
    def get_atten_volts(self):
        """
        Get the voltages associated with each attenuator.
        Returns:
            dict: keys are attenuator names, values are voltages for each attenuator
        """
        report = {}
        for name in self.get_atten_names():
            self.serverlog.debug("get_atten_volts: PM attenuator name: {}".format(name))
            if not self._simulated:
                val = self.wbdc.get_atten_volts(name)
            else:
                val = random.random()
            report[name] = val
            # self.serverlog.debug("get_atten_volts: {}: {}".format(name, val))
        self.serverlog.debug("get_atten_volts: Volts: {}".format(report))
        return report

    @AutoTestAnnotation()
    def get_pm_atten_volts(self):
        """
        Get the voltages associated with only power meter attenuators
        Returns:

        """
        report = {}
        for name in self._pm_att:
            atten_name = self._pm_att[name]
            self.serverlog.debug("get_pm_atten_volts: PM attenuator key, value: {}, {}".format(name, atten_name))
            if not self._simulated:
                val = self.wbdc.get_atten_volts(atten_name)
            else:
                val = random.random()
            report[atten_name] = val
        self.serverlog.debug("get_pm_atten_volts: Volts: {}".format(report))
        return report

    @AutoTestAnnotation(args=('R1-24-E', 5.0))
    @error_decorator
    def set_atten(self, atten_name, value):
        """

        Args:
            atten_name (str): The name of the attenuator (from self.get_atten_names)
            value (float): The value we want to set
        Returns:
            None
        """
        if value:
            self.serverlog.debug("set_atten: setting {} to {:.2f}".format(atten_name, value))
            if not self._simulated:
                if value:
                    self.wbdc.set_atten(atten_name, value)
            else:
                self.simulated_atten[atten_name][0] = value
        else:
            self.serverlog.debug("set_atten: Can't set value None for attenuator {}".format(atten_name))

    @AutoTestAnnotation(args=(1, 5.0))
    @error_decorator
    def set_pm_atten(self, atten_id, value):
        """
        Set PIN diode attenuator for IF channel

        Args:
            atten_id (int or str): IF channel number. This corresponds to one of the keys
                of the result of self.attenuator_names.
                OR can be the actual name of the attenuator.
            value (float): The value to set the attenuator to.
        Returns:
            None
            # str: Descriptive of the state
        """
        if isinstance(atten_id, str) and atten_id in self._pm_att.values():  # the actual attenuator name
            att = atten_id
        else:
            att = self._pm_att[atten_id]
        self.serverlog.debug("set_pm_atten: setting {} to {:.2f}".format(att, value))
        self.set_atten(att, value)

    @AutoTestAnnotation(args=([5.0, 5.0, 5.0, 5.0], ))
    @error_decorator
    def set_pm_attens(self, vals):
        """
        Set all the pm attenuators at once.
        Args:
            vals:

        Returns:
        """
        for i in xrange(1,5):
            self.set_pm_atten(i, vals[i-1])

    @AutoTestAnnotation()
    def init_pms(self):
        """
        Initializes the Hewlett Packard (Agilent) power meters
        """
        if not self._simulated:
            self.serverlog.debug("init_pms: Initializing Power Meters")
            self.FE.init_pms()
        else:
            self.serverlog.debug("init_pms: Called")

    @AutoTestAnnotation()
    def read_pms(self):
        """
        The power meters are read in the order 1, 2, 3, 4.  The corresponding
        receiver outputs are specified by method _pm_patching_sources()
        """
        if not self._simulated:
            try:
                readings = self.FE.read_pms()
                self.serverlog.debug("read_pms: readings: {}".format(readings))
            except Exception, details:
                self.serverlog.error("read_pms: failed due to %s", details)
            return readings
        else:
            readings = []
            for i in xrange(1,5):
                readings.append((i, datetime.datetime.utcnow().isoformat(), random.random()))
            return readings

    @AutoTestAnnotation()
    def get_tsys_factors(self):
        """
        Get the tsys factors that allow for conversion between raw power meter output
        and sky temperature
        Returns:
            dict: Keys are power meter names ('PM1', etc), values are tsys factors
        """
        return {'PM1': self.tsysfactor1,
                'PM2': self.tsysfactor2,
                'PM3': self.tsysfactor3,
                'PM4': self.tsysfactor4}

    @AutoTestAnnotation()
    def get_tsys(self):
        """
        Get the tsys given the current tsysfactors.
        Returns:
            dict: with tsys and pm_readings as keys.
        """
        readings = self.read_pms()
        tsys = []
        pm_readings = []
        for i in xrange(4):
            tsysfactor = getattr(self, "tsysfactor{}".format(i+1))
            pm_reading = readings[i][-1]
            pm_readings.append(pm_reading)
            tsys.append(float(pm_reading) * tsysfactor)
        self.serverlog.debug('get_tsys: Current tsys values: {}'.format(tsys))
        return {'tsys':tsys,
                'pm_readings':pm_readings}

    @AutoTestAnnotation()
    def read_temp(self):
        """Read Front end rx temp"""
        if not self._simulated:
            temp = self.FE.read_temp()
            return temp
        else:
            return None

    @AutoTestAnnotation()
    @error_decorator
    def get_feed_state(self):
        """
        Get the current feed state of the FrontEnd.
        Returns:
            list: Feed 1 and Feed 2 state (sky or load)
        """
        if not self._simulated:
            feed_state = self._set_WBDC(12)
            feed_state = feed_state.split("\n")
            feed_state = [line.split(" ")[-1] for line in feed_state[1:-1]]
            return feed_state
        else:
            return [self.simulated_feed_state[1], self.simulated_feed_state[2]]

    @AutoTestAnnotation(args=(1, 'sky'))
    def set_feed_state(self, feed, state):
        """
        Args:
            feed (int): The number of the feed
            state (str): The state to set ('load' or 'sky')
        """
        state = state.lower().strip()
        if state != 'load' and state != 'sky':
            error_msg = "Specified state is {}; not either load or sky".format(state)
            self.serverlog.error(error_msg)
            raise ValueError(error_msg)
        else:
            if not self._simulated:
                try:
                    if feed == 1 and state == 'sky':
                        self.serverlog.debug("Setting feed 1 to sky")
                        return self._set_WBDC(13)
                    elif feed == 1 and state == 'load':
                        self.serverlog.debug("Setting feed 1 to load")
                        return self._set_WBDC(14)
                    elif feed == 2 and state == 'sky':
                        self.serverlog.debug("Setting feed 2 to sky")
                        return self._set_WBDC(15)
                    elif feed == 2 and state == 'load':
                        self.serverlog.debug("Setting feed 2 to load")
                        return self._set_WBDC(16)
                except Exception as err:
                    error_msg = "Error setting feed state: {}".format(err)
                    self.serverlog.error(error_msg, exc_info=True)
                    raise Pyro4ServerError(error_msg)
            else:
                self.simulated_feed_state[feed] = state

    @error_decorator
    def get_y_factor(self):
        """
        Currently doesn't work
        Returns:

        """
        if not self._simulated:
            return self._set_WBDC(18)
        else:
            self.serverlog.error("Simulator doesn't have Y-factor")
            return None

    @AutoTestAnnotation()
    @error_decorator
    def get_noise_diode_state(self):
        """
        Get the current state of the noise diode
        Returns:
            bool: Whether noise diode is on or off
        """
        if not self._simulated:
            resp = self._set_WBDC(22)
            if resp == 'on':
                return True
            elif resp == 'off':
                return False
        else:
            return self.simulated_noise_diode_state

    @AutoTestAnnotation(args=(False, ))
    @error_decorator
    def set_noise_diode_state(self, state):
        """
        Set the state of the noise diode (on or off)
        Args:
            state (bool): On or off
        Returns:
            None
        """
        if not self._simulated:
            if state:
                resp = self._set_WBDC(23)
            else:
                resp = self._set_WBDC(24)
            self.serverlog.debug("set_noise_diode_state: Response from FrontEnd hardware server: {}".format(resp))
        else:
            self.simulated_noise_diode_state = state

    @AutoTestAnnotation(args=(1, True))
    @error_decorator
    def set_preamp_bias(self, feed, state):
        """
        Set the pre-amp bias
        Args:
            feed (int): The number of the feed to set
            state (bool): On or off

        """
        if not self._simulated:
            if feed == 1 and state:
                self.serverlog.debug("Turning feed 1 pre-amp bias on")
                return self._set_WBDC(25)
            elif feed == 1 and not state:
                self.serverlog.debug("Turning feed 1 pre-amp bias off")
                return self._set_WBDC(26)
            elif feed == 2 and state:
                self.serverlog.debug("Turning feed 2 pre-amp bias on")
                return self._set_WBDC(27)
            elif feed == 2 and not state:
                self.serverlog.debug("Turning feed 2 pre-amp bias off")
                return self._set_WBDC(28)
        else:
            self.simulated_preamp_bias[feed] = state

    @AutoTestAnnotation()
    @error_decorator
    def get_front_end_temp(self):
        """
        Return the front end temperature
        Returns:
            dict: load 1, load 2, 12K and 70K temp
        """
        if not self._simulated:
            resp = self._set_WBDC(31)
            return eval(resp)
        else:
            return {
                'load1': random.random(),
                'load2': random.random(),
                '12K':random.random(),
                '70K':random.random()
            }

    @AutoTestAnnotation()
    @error_decorator
    def get_analog_data(self):
        """
        Get analog data from the WBDC
        Returns:
            dict: Information about the WBDC.
        """
        if not self._simulated:
            resp = self._set_WBDC(38)
            return resp
        else:
            return {'+12 V': random.random(), '+16 V': random.random(), '+16 V LDROs': random.random(),
                    '+16 V MB': random.random(), '+16 V R1 BE': random.random(), '+16 V R1 FE': random.random(),
                    '+16 V R2 BE': random.random(), '+16 V R2 FE': random.random(), '+6 V R1 FE': random.random(),
                    '+6 V R2 FE': random.random(), '+6 V ana': random.random(), '+6 V analog MB': random.random(),
                    '+6 V dig': random.random(), '+6 V digitalMB':  random.random(), '-16 V': random.random(),
                    '-16 V MB': random.random(), '-16 V R1 BE': random.random(), '-16 V R1 FE': random.random(),
                    '-16 V R2 BE': random.random(), '-16 V R2 FE': random.random(), 'BE plate': random.random(),
                    'R1 E-plane': random.random(), 'R1 H-plane': random.random(), 'R1 RF plate': random.random(),
                    'R2 E-plane': random.random(), 'R2 H-plane': random.random(), 'R2 RF plate': random.random()}

    @AutoTestAnnotation()
    @error_decorator
    def get_crossover_switch(self):
        """
        Get the cross over switch state in the WBDC
        Returns:
            dict:
                keys: 1 or 2 (the crossover switch)
                values: True or False
        """
        if not self._simulated:
            cross_switch_state = self.wbdc.get_Xswitch_states()
            return cross_switch_state
        else:
            return self.simulated_crossover_switch_state

    @AutoTestAnnotation(args=(False, ))
    @error_decorator
    def set_crossover_switch(self, state):
        """
        Set the crossover switch in the WBDC
        Args:
            state (bool): Whether to set or unset the crossover switch
        Returns:
            None
        """
        if not self._simulated:
            if state:
                resp = self._set_WBDC(41)
            else:
                resp = self._set_WBDC(42)
            self.serverlog.debug("set_crossover_switch: Response from server: {}".format(resp))
        else:
            self.simulated_crossover_switch_state[1] = state

    @AutoTestAnnotation()
    @error_decorator
    def get_polarizers(self):
        """
        Get the polorization state. 1 is circular, 0 is linear
        Returns:
            dict:
        """
        if not self._simulated:
            resp = self.wbdc.get_pol_sec_states()
            polarizer_mode = list(set(resp))
            if len(polarizer_mode) == 1:
                if polarizer_mode[0] == 0:
                    self._polarizer_mode = 'linear'
                elif polarizer_mode[0] == 1:
                    self._polarizer_mode = 'circular'
            return resp
        else:
            val = None
            if self._polarizer_mode.lower().strip() == 'linear':
                val = 0
            elif self._polarizer_mode.lower().strip() == 'circular':
                val = 1
            return {'R1-18': val, 'R1-20': val, 'R1-22': val, 'R1-24': val, 'R1-26': val,
                    'R2-18': val, 'R2-20': val, 'R2-22': val, 'R2-24': val, 'R2-26': val}


    @error_decorator
    @ParserDecorator(['circular', 'linear'], [43, 44], "_polarizer_mode")
    def set_polarizers(self, opt):
        """
        Set the polarizers to either circular or linear
        Args:
            opt (int): The opt to pass to self._set_WBDC
        Returns:
            dict: polarizer state, value of 1 corresponds to circular, 0 to linear
        """
        if not self._simulated:
            resp = self._set_WBDC(opt)
            return resp
        else:
            val = None
            if self._polarizer_mode.lower().strip() == 'linear':
                val = 0
            elif self._polarizer_mode.lower().strip() == 'circular':
                val = 1
            return {'R1-18': val, 'R1-20': val, 'R1-22': val, 'R1-24': val, 'R1-26': val,
                    'R2-18': val, 'R2-20': val, 'R2-22': val, 'R2-24': val, 'R2-26': val}

    @AutoTestAnnotation()
    @error_decorator
    def get_IF_hybrids(self):
        """
        Get the IF hybrids state. 1 is IQ, 0 is LU
        Returns:
            dict: IF hybrid state
        """
        if not self._simulated:
            resp = self.wbdc.get_DC_states()
            if_hybrid_state = list(set(resp))
            if len(if_hybrid_state) == 1:
                if if_hybrid_state[0] == 0:
                    self._IF_hybrid_mode = 'lu'
                elif if_hybrid_state[0] == 1:
                    self._IF_hybrid_mode = 'iq'

            return resp
        else:
            val = None
            if self._IF_hybrid_mode.lower().strip() == 'iq':
                val = 1
            elif self._IF_hybrid_mode.lower().strip() == 'lu':
                val = 0
            return {'R1-18P1': val, 'R1-18P2': val, 'R1-20P1': val, 'R1-20P2': val, 'R1-22P1': val,
                    'R1-22P2': val, 'R1-24P1': val, 'R1-24P2': val, 'R1-26P1': val, 'R1-26P2': val,
                    'R2-18P1': val, 'R2-18P2': val, 'R2-20P1': val, 'R2-20P2': val, 'R2-22P1': val,
                    'R2-22P2': val, 'R2-24P1': val, 'R2-24P2': val, 'R2-26P1': val, 'R2-26P2': val}

    @error_decorator
    @ParserDecorator(['iq', 'ul'], [45, 46], "_IF_hybrid_mode")
    def set_IF_hybrids(self, opt):
        """
        Set the IF hybrids to IQ or LU
        Args:
            opt (int): The opt to pass to self._set_WBDC
        Returns:
            dict: IF hybrid state, value of 1 corresponds to IQ, val to LU
        """
        if not self._simulated:
            resp = self._set_WBDC(opt)
            return resp
        else:
            val = None
            if self._IF_hybrid_mode.lower().strip() == 'iq':
                val = 1
            elif self._IF_hybrid_mode.lower().strip() == 'ul':
                val = 0
            return {'R1-18P1': val, 'R1-18P2': val, 'R1-20P1': val, 'R1-20P2': val, 'R1-22P1': val,
                    'R1-22P2': val, 'R1-24P1': val, 'R1-24P2': val, 'R1-26P1': val, 'R1-26P2': val,
                    'R2-18P1': val, 'R2-18P2': val, 'R2-20P1': val, 'R2-20P2': val, 'R2-22P1': val,
                    'R2-22P2': val, 'R2-24P1': val, 'R2-24P2': val, 'R2-26P1': val, 'R2-26P2': val}

    @error_decorator
    @ParserDecorator(['w', 'db'], [[391, 392, 393, 394], [401, 402, 403, 404]], "_PM_mode")
    def set_PM_mode(self, opts):
        """
        Set the power meter mode, either W or dB.
        Args:
            opts (list): List of opts to pass to self._set_WBDC
        Returns:
            None
        """
        if not self._simulated:
            for opt in opts:
                resp = self._set_WBDC(opt)
                self.serverlog.debug("set_PM_mode: Response from server: {}".format(resp))
        else:
            self.serverlog.debug("set_PM_mode: ")

    def retrieve_previous_minical_results(self, filename=None):
        """
        Retrieve old minical results
        Args:
            filename (str): The name of the JSON file that contains the results
        Returns:
            'return_vals' from self.perform_minical
        """
        self.serverlog.info("Retrieving old minical results.")
        if not filename: filename = self.settings_file
        try:
            with open(filename, 'r') as f:
                minical = json.load(f)['minical']
                for i in xrange(4):
                    setattr(self, "tsysfactor{}".format(i+1), minical['tsysfactors'][i])
                return minical
        except IOError as err:
            self.serverlog.error("Couldn't find or read settings file.")
        except ValueError as err:
            self.serverlog.error("Couldn't find minical results in settings file.")
        except Exception as err:
            self.serverlog.error("Couldn't get previous minical results: {}".format(err), exc_info=True)

    def perform_minical(self, q=None, save_config=True):
        """
        Perform minical. This calibrates the power meters in the Front End,
        creating a correspondance between Power meter readings and sky temperature.

        args:
            q (queue.Queue.Queue): this allows us to retrive values calculated while this function runs.
                OR, if we're using this inside a PyQt thread, this will be None
        return (Doesn't get 'returned' -- gets 'put' in the Queue instance):
            dict containing the following keys/values:
            'tsys_pm': a list of the current power meter system temperatures
            'x': The values to be plotted on an x axis
            'Tlinear': The linear fit, from what I can tell
            'Tquadratic': The quadratic fit.

        """
        self.serverlog.info("Performing minical...")
        return_vals = None
        if not self._simulated:
            try:
                results = self._set_WBDC(29)
                gains = results[0]
                Tlinear = results[1]
                Tquadratic = results[2]
                Tnd = results[3]
                NonLin = results[4]
                x = results[5]
                self.serverlog.info("Minical : gain calibrated: {}".format(gains))
                self.serverlog.info("Minical : Linear Ts: {}".format(Tlinear))
                self.serverlog.info("Minical : Corrected Ts: {}".format(Tquadratic))
                self.serverlog.info("Minical : Noise Diode T: {}".format(Tnd))
                self.serverlog.info("Minical : Nonlinearity: {}".format(NonLin))
                self.serverlog.info("Minical : Calibrated reading: {}".format(x))
                color1 = ['r', 'b', 'g', 'purple']
                self.serverlog.info("Minical : Minical performed; Corrected PM readings-{}".format(str(x)))
                self.serverlog.info("Minical : Noise diode temperatures-{}".format(str(Tnd)))
                read_pm1, read_pm2, read_pm3, read_pm4 = x[0][0], x[1][0], x[2][0], x[3][0]
                tsys_pm1, tsys_pm2, tsys_pm3, tsys_pm4 = Tquadratic[0][0], Tquadratic[1][0], Tquadratic[2][0], \
                                                         Tquadratic[3][0]
                self.serverlog.info(
                    "Minical : Tsys for PM1 {}, PM2 {}, PM3 {}, PM4 {}".format(tsys_pm1, tsys_pm2, tsys_pm3, tsys_pm4))
                self.tsysfactor1 = tsys_pm1 / read_pm1
                self.tsysfactor2 = tsys_pm2 / read_pm2
                self.tsysfactor3 = tsys_pm3 / read_pm3
                self.tsysfactor4 = tsys_pm4 / read_pm4
                self.serverlog.info(
                    "Minical : Minical derived tsys factors ,{},{},{},{}".format(self.tsysfactor1, self.tsysfactor2,
                                                                                 self.tsysfactor3, self.tsysfactor4))
                return_vals = {
                                'tsysfactors': [self.tsysfactor1, self.tsysfactor2,
                                                self.tsysfactor3, self.tsysfactor4],
                                'tsys_pm': [tsys_pm1, tsys_pm2, tsys_pm4, tsys_pm4],
                                'x': x,
                                'Tlinear': Tlinear,
                                'Tquadratic': Tquadratic
                }


                    # self.serverlog.error("Saving minical results not yet implemented.")


            except Exception as err:
                self.serverlog.error("Couldn't perform minical. Error: {}".format(err), exc_info=True)

        if save_config:
            self.serverlog.debug("Saving minical data to {}".format(self.settings_file))
            with open(self.settings_file, 'r+') as f:
                try:
                    data = json.load(f)
                    data.update({'minical': return_vals})
                    f.seek(0)
                    json.dump(data, f)
                    f.truncate()
                except ValueError as err:
                    self.serverlog.debug("Error updating file: {}".format(err))
                    f.seek(0)
                    json.dump({'minical': return_vals}, f)
                    f.truncate()


        if not q:
            return return_vals
        else:
            q.put(return_vals)
#
#
# class TestClass(object):
#     def __init__(self):
#         self._test_mode = "W"
#         self.serverlog = logging.getLogger(module_logger.name + ".TestClass")
#
#     @ParserDecorator(['w', 'db'], [1, 2])
#     def test_method(self, opt):
#         return opt
#
#     @error_decorator
#     @ParserDecorator(['w', 'db'], [1, 2], "_test_mode")
#     def test_method1(self, opt):
#         raise RuntimeError("Oops")
#         # return opt


if __name__ == '__main__':
    pass