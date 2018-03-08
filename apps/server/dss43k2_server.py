"""
"""
import datetime
import time
import os
import threading
import logging
import inspect
import json
import astropy

import numpy as np
import scipy.optimize as op
from scipy import constants
import ephem
import h5py
import Pyro4

from tams_source import TAMS_Source
from support.threading_util import PausableThread, iterativeRun
from support.pyro import Pyro4Server, config, async
from support.trifeni import NameServerTunnel
from support import weather
from MonitorControl.Configurations.CDSCC import FO_patching
import MonitorControl.Configurations.coordinates as coord

from .workers import TwoBeamNodWorker, RMSWorker, APCWorker, PowerMeterWorker, LongRunningWorker
from .data_acquisition import TAMSHDF5File

Pyro4.config.COMMTIMEOUT = 0.0
c_kms = astropy.constants.c.to('km/s').value
c = astropy.constants.c

@config.expose
class DSS43K2Server(Pyro4Server):
    """
    Class for integrating hardware server control
    We can directly interact with hardware servers,
    or we can call methods that might integrate functionality
    from some or all of the servers.

    For functionality to stay server side, it needs to be inside a method here.
    We can call methods directly on the hardware (APC, WBDC, Spectromter) servers,
    but we need the appropriate tunnels in place.
    However, if we have methods here that call hardware server methods then it calls these methods
    internally. This is to say that if we have a method DSS43K2Server.get_azel that simply calls
    the self.apc.get_azel method, that is NOT the same as calling dss43k2_proxy.apc.get_azel directly.
    This is due to the way that Pyro does method calls.

    WBDC/Radiometer configuration notes:

    The way the WBDC and radiometer is setup in Canberra is such that
    that power meter data only corresponds to specific feeds from the antenna.
    For example, radiometer head 1 might be hooked up to 22 GHz band, feed 1,
    upper polarization, lower sideband.

    We do not have software control of this configuration. In order to know the
    current hardware configuration, we read an "FO_Patching.xlsx" file
    (fiber optic patching) that tells us which WBDC power meter/radiometer heads
    are hooked up to what, AND which ROACHs are hooked up to these heads.

    In the end the idea is that this master server knows the configuration,
    such that when we call the public "get_pms" method, we are getting powermeter
    data corresponding to the ROACHs.

    FO_patching notes:

    We can use the FO_patching module as follows (can be found in MonitorControl.Configurations.CDSCC.FO_patching):

    ```
    dist_assembly = FO_patching.DistributionAssembly()
    # get the Power Meters configuration in the FrontEnd
    dist_assembly.get_signals("Power Meter")
    # get the Radiometer configuration
    dist_assembly.get_signals("Radiometer")
    # get the ROACH1 configuration
    dist_assembly.get_signals("ROACH1")
    >> {u'SAO1': {'Band': 22, 'IF': u'L', 'Pol': u'E', 'Receiver': 1},
         u'SAO2': {'Band': 22, 'IF': u'L', 'Pol': u'H', 'Receiver': 1},
         u'SAO3': {'Band': 22, 'IF': u'L', 'Pol': u'E', 'Receiver': 2},
         u'SAO4': {'Band': 22, 'IF': u'L', 'Pol': u'H', 'Receiver': 2}}
    ```

    Example Usage:

    We might start the server as follows:

    ```
    server = DSS43K2Server()
    server.launch_server(ns_port=50000)
    ```
    """
    # source_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sources.json")
    # source_dir = "/usr/local/projects/TAMS/Observations/dss43/"
    source_dir = "/home/ops/dean/sources/"
    # source_dir = "/home/dean/jpl-dsn/sources/"

    def __init__(self,
                name='DSS43',
                remote_server_name='localhost',
                ns_host='localhost',
                ns_port=50000,
                remote_port=22,
                local_forwarding_port=None,
                local=True,
                remote_username='ops',
                logfile=None,
                simulated=False,
                callback_handler_uri=None,
                logger=None):
        """
        Connect to the APC, Spectrometer, FrontEnd/WBDC and Radiometer servers, and set up
        the roach/power meter correspondance.
        """
        self._simulated = simulated
        super(DSS43K2Server, self).__init__(obj=self, name=name, logfile=logfile, logger=logger)
        self._ns_tunnel = NameServerTunnel(remote_server_name=remote_server_name,
                                    local=local,
                                    ns_host=ns_host,
                                    ns_port=ns_port,
                                    remote_port=remote_port,
                                    create_tunnel_kwargs=None,
                                    remote_username=remote_username)
        self.logger.info("__init__: self.logger.level: {}".format(self.logger.level))
        self.internal_server_thread = None

        self.subscriber_ns_host = ns_host
        self.subscriber_ns_port = ns_port
        # set up the distribution assembly
        if self._simulated:
            # parampath = '/home/dean/jpl-dsn/'
            # self.dist_assmbly = FO_patching.DistributionAssembly(parampath=parampath)
            self.dist_assmbly = FO_patching.DistributionAssembly()
            self._data_dir = "/home/dean/jpl-dsn/dataFiles"
            from MonitorControl.Configurations.CDSCC.apps.server.wbdc_server import WBDCFrontEndServer
            from MonitorControl.BackEnds.ROACH1.apps.server.SAO_pyro4_server import SpectrometerServer
            from MonitorControl.Antenna.DSN.apps.server.apc_server import APCServer

            self._apc = APCServer(wsn=0,site='CDSCC',dss=43,
                                   simulated=self._simulated,
                                   logfile=self.logfile)
            self._spec = SpectrometerServer(simulated=self._simulated, synth=None, logfile=self.logfile)
            self._wbdc_fe = WBDCFrontEndServer(simulated=self._simulated,
                                               logfile=self.logfile)
                                            #    patching_file_path=parampath,
                                               # settings_file="/home/dean/jpl-dsn/.WBDCFrontEndsettings.json")
            self._hppm = None
            self._rad = None
        elif not self._simulated:
            self.dist_assmbly = FO_patching.DistributionAssembly()
            self._data_dir = "/home/ops/roach_data/sao_test_data/data_dir"
            # Establish connections to all the servers.
            self._proxies = {"apc": "APC",
                             "spec":"Spec",
                             "wbdc_fe":"WBDC_FE"}

            for attr_name in self._proxies:
                try:
                    self.connect_to_proxy(attr_name)
                except Exception as err:
                    self.logger.error("Couldn't connect to {}: {}".format(self._proxies[attr_name], err), exc_info=True)

            try:
                krx43_ns = Pyro4.locateNS(host="krx43",port=9091)
                self._hppm = Pyro4.Proxy(krx43_ns.lookup("HPPM"))
            except Exception as err:
                self.logger.error("Couldn't connect to HP power meter server. Error {}".format(err))
                self._hppm = None
            try:
                rad_ns = Pyro4.locateNS(host="tpr",port=9091)
                self._rad = Pyro4.Proxy(rad_ns.lookup("Radiometer"))
            except Exception as err:
                self.logger.error("Couldn't connect to Radiometer server. Error {}".format(err))
                self._rad = None

        self.roach_corr = self.power_meter_correspondence()

        self.df = None
        cdscc = coord.DSS(43)
        cdscc.epoch = ephem.J2000
        cdscc.date = datetime.datetime.utcnow()
        self._cdscc = cdscc

        self.logger.debug("Current data directory: {}".format(self._data_dir))
            # self._data_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # default file path.
        doy = str(datetime.datetime.utcnow().timetuple().tm_yday)
        project_dir = os.path.join(self._data_dir, doy)
        self._project_dir = project_dir

        # create attributes for any worker threads.
        self.twobeamworker = None
        self.pm_recording_worker = None
        self.apc_recording_worker = None
        self.rms_recording_worker = None
        self.apc_info = {}
        self.tsys_info = {}
        self.rms_info = {}

        if callback_handler_uri:
            self.set_callback_handler(callback_handler_uri) # this method sets the attribute internally
        else:
            self.cb_handler = None

        if self._simulated:
            offsets = {'el':0.0, "xel":0.0}
        else:
            offsets = self._apc.get_offsets()

        self._boresight_info = {'running': False}
        self._boresight_offset_el = offsets['el']
        self._boresight_offset_xel = offsets['xel']
        self.logger.debug("Initial boresight offsets: {}, {}".format(self._boresight_offset_el, self._boresight_offset_xel))
        self._pointing_info = {'running': False}
        self._data_acquisition_info = {'running': False}
        self._tipping_info = {'running': False}
        self._minical_info = {'running': False}

    def get_version_info(self):
        """Get DSS43Backend version"""
        # from .. import __version__
        # return __version__
        return "1.0.0"

    def get_logfiles(self):
        """Get the current logfiles for all servers"""
        return {'apc':self.apc.logfile,
                'spec':self.spec.logfile,
                'dss43':self.logfile,
                'wbdc_fe':self.wbdc_fe.logfile}

    @property
    def simulated(self):
        return self._simulated

    @property
    def project_dir(self):
        return self._project_dir

    @property
    def data_dir(self):
        return self._data_dir

    @property
    def proxies(self):
        return self._proxies

    @property
    def apc(self):
        return self._apc

    @property
    def spec(self):
        return self._spec

    @property
    def wbdc_fe(self):
        return self._wbdc_fe

    @property
    def boresight_info(self):
        return self._boresight_info

    def set_boresight_running(self, flag):
        self.logger.debug("set_boresight_running: Setting self._boresight_info['running'] to {}".format(flag))
        self._boresight_info['running'] = flag

    @property
    def data_acquisiton_info(self):
        return self._data_acquisition_info

    @property
    def pointing_info(self):
        return self._pointing_info

    def set_pointing_running(self, flag):
        self.logger.debug("set_pointing_running: Setting self._pointing_info['running'] to {}".format(flag))
        self._pointing_info['running'] = flag

    @property
    def tipping_info(self):
        return self._tipping_infow

    def set_tipping_running(self, flag):
        self.logger.debug("set_tipping_running: Setting self._tipping_info['running'] to {}".format(flag))
        self._tipping_info['running'] = flag

    @property
    def minical_info(self):
        return self._minical_info

    def set_minical_running(self, flag):
        self.logger.debug("set_minical_running: Setting self._minical_info['running'] to {}".format(flag))
        self._minical_info['running'] = flag

    @property
    def boresight_offset_el(self):
        return self._boresight_offset_el

    @property
    def boresight_offset_xel(self):
        return self._boresight_offset_xel

    @Pyro4.oneway
    @async.async_method
    def get_initial_state(self):
        self.logger.debug("get_initial_state: Called.")
        self.get_initial_state.cb({'boresight':self._boresight_info,
                'data_acquisition':self._data_acquisition_info,
                'pointing': self._pointing_info,
                'minical': self._minical_info,
                'tipping': self._tipping_info})

    @Pyro4.oneway
    @async.async_method
    def set_callback_handler(self, callback_handler_uri):
        """
        Set the callback handler attribute. This is a Pyro Proxy that we use to call callbacks.
        Args:
            callback_handler (Pyro4.core.URI):
        Returns:
            None
        """
        self.logger.debug("Setting new callback handler. Type: {}".format(type(callback_handler_uri)))
        if isinstance(callback_handler_uri, Pyro4.core.Proxy):
            self.cb_handler = callback_handler_uri
            self.logger.warning("Won't be able to automatically reconnect if using simply Pyro4 Proxy.")
        else:
            self.cb_handler = Pyro4.Proxy(callback_handler_uri)
        self.logger.debug("Attempting to wait for availablity of reverse tunnel.")
        while True:
            try:
                self.set_callback_handler.cb()
                break
            except Pyro4.errors.CommunicationError:
                self.logger.debug("Reverse tunnel not yet established. Timing out...")
                time.sleep(1.0)

    def hdwr_method(self, controller, method_name, *args, **kwargs):
        """
        Call a method from one of the hardware controllers (eg, APC, spectrometer, etc)
        Args:
            controller (str): one of the hardware controllers, like the APC. Should be the name of the attribute.
            method_name (str): The method name to call.
            *args: For controller.method_name
            **kwargs: For controller.method_name
        Returns:
            results from controller.method_name
        """
        try:
            proxy = getattr(self, controller)
        except AttributeError as err:
            self.logger.error("Couldn't get attribute {}: {}".format(controller, err))
        if not self._simulated:
            try:
                method = getattr(proxy, method_name)
                if isinstance(method, Pyro4.core._RemoteMethod):
                    return method(*args, **kwargs)
                else:
                    return method # means we're accessing an attribute, not a method.
            except AttributeError as err:
                self.logger.error("Couldn't get the remote method {}: {}".format(method_name, err))
            except Exception as err:
                self.logger.error("Remote method call {}.{} failed: {}".format(controller, method_name, err), exc_info=True)
        else:
            try:
                method = getattr(proxy, method_name)
                if callable(method):
                    return method(*args, **kwargs)
                else:
                    return method
            except Exception as err:
                self.logger.error("Couldn't access method {}: {}".format(method_name, err))

    @Pyro4.oneway
    @async.async_method
    def hdwr_method_async(self, controller, method_name, *args, **kwargs):
        """asynchronously call the hdwr_method method"""
        response = self.hdwr_method(controller, method_name, *args, **kwargs)
        self.hdwr_method.cb({"status":"", "response":response})

    def check_connections(self):
        """
        Check the connections to each of the proxies. Note that this will bind, or create a connection
        to each of the proxies (if not already done)
        Returns:
            dict: Keys are the names of the proxy servers, and values are booleans.
        """
        for name in self._proxies[name]:
            getattr(self, "_{}".format(name)).ping()

    def connect_to_proxy(self, name):
        """
        Attempt to connect to a proxy.
        Args:
            name (str): The name of the server to which we'd like to establish a connection
        """
        proxy_name = self._proxies[name]
        proxy = self._ns_tunnel.get_remote_object(proxy_name)
        self.logger.debug("Proxy name: {}, Proxy info {}".format(proxy_name, proxy))
        setattr(self, "_{}".format(name), proxy)

    def close(self):
        """
        Reimplemented from Pyro4Server.
        """
        # self._ns_tunnel.cleanup()
        Pyro4Server.close(self)

    def roach_patching(self):

        return self.dist_assmbly.get_signals("ROACH1")

    def power_meter_patching(self):

        return self.dist_assmbly.get_signals("Power Meter")

    def radiometer_patching(self):

        return self.dist_assmbly.get_signals("Radiometer")

    def power_meter_correspondence(self):
        """
        Establish which ROACHs and Radiometer/FE power meters have the same
        patching configuration.
        Returns:
            dict: Keys are name of roachs, values are names of power meters.
        """
        roach_assmbly = self.dist_assmbly.get_signals('ROACH1')
        rad_assmbly = self.dist_assmbly.get_signals('Radiometer')
        FE_assmbly = self.dist_assmbly.get_signals('Power Meter')

        pm_assmbly = dict(FE_assmbly)
        pm_assmbly.update(rad_assmbly)

        roach_correspondance = {}
        for roach_name in roach_assmbly:
            for pm_name in pm_assmbly:
                if roach_assmbly[roach_name] == pm_assmbly[pm_name]:
                    roach_correspondance[roach_name] = pm_name
                    self.logger.info("PM for {}: {}".format(roach_name, pm_name))

        return roach_correspondance

    # ==================================================Minical=========================================================
    def process_minical_calib(self, cal_data, Tlna=25, Tf=1, Fghz=20, TcorrNDcoupling=0):
        """
        Process minical calibration data.
        Args:
            cal_data (dict): The result of self.minical
        Returns:
            list:
                gains (list of lists): power meter gains
                Tlinear (list of lists): Linear fit paramters
                Tquadratic (list of lists): Quadratic fit parameter
                Tnd (list of floats): Noise diode temperature
                NonLin (list of floats): Non linear component
        """
        def gain_cal(Tlna, Tf, R1, R2, R3, R4, R5, Tp, Fghz, TP4corr):
            """
            Computes minical parameters from minical data

            This uses the Stelreid/Klein calibration algorithm
            Stelzried and Klein, Proc. IEEE, 82, 776 (1994) to
            compute B, BC and CC, as well as Tnd.
            The true gain is B ( BC*R + B*CC*R^2 )

            Args:
                Tlna (float): the LNA noise temperature
                Tf (float): follow-on amplifier noise temperature contribution
                R1 (float): reading with power meter input zeroed
                R2 (float): reading with LNA connected to antenna
                R3 (float): reading with LNA connected to antenna, noise diode on
                R4 (float): reading with LNA connected to ambient load
                R5 (float): reading on ambient load, noise diode on
                Tp (np.ndarray): physical temperature of ambient load (deg K)
                Fghz (float): frequency in GHz
                TP4corr (float): correction to T4 due to VSWR at the ambient load
            Returns:
                tuple:
                      B - linear or mean gain
                      BC - linear component of second order gain
                      CC - quadratic component of second order gain
                      Tnd - noise diode temperature (K)
            """
            # correction from Rayleigh-Jeans approx. to Planck
            Tc = -0.024 * Fghz
            # system temperature on the load
            T4P = Tp + Tlna + Tf + Tc + TP4corr
            # This is the gain, assuming the system is linear:
            # 	T = B * ( R - R1 )
            B = T4P / (R4 - R1)
            T2 = B * (R2 - R1)  # linear system temperature on sky
            T3 = B * (R3 - R1)  # linear system temperature on sky with noise diode
            T5 = B * (R5 - R1)  # linear system temperature in load with noise diode
            M = T5 * T5 - T4P * T4P - T3 * T3 + T2 * T2
            N = T5 - T4P - T3 + T2
            CC = N / (N * T4P - M)
            BC = 1.0 - CC * T4P
            Tnd = BC * (T3 - T2) + CC * (T3 * T3 - T2 * T2)
            return B, BC, CC, Tnd

        R1 = np.array(cal_data['zero'])
        R2 = np.array(cal_data['sky'])
        # if R2 == 0:
        #     print "process_minical result for R2: $R2"
        #     return None
        R3 = np.array(cal_data['sky+ND'])
        R4 = np.array(cal_data['load'])
        R5 = np.array(cal_data['load+ND'])
        load = np.array(cal_data['Tload'])
        # pm_mode = cal_data['mode']
        # if pm_mode == "dBm":
        #     convert dBm to W
            # R2 = math.pow(10.0, R2 / 10.0) / 1000.0
            # R3 = math.pow(10.0, R3 / 10.0) / 1000.0
            # R4 = math.pow(10.0, R4 / 10.0) / 1000.0
            # R5 = math.pow(10.0, R5 / 10.0) / 1000.0
        gains = gain_cal(Tlna, Tf, R1, R2, R3, R4, R5,
                         load, Fghz, TcorrNDcoupling)
        self.logger.debug("process_minical_calib: gain_cal returned B, BC, CC, Tnd: {}".format(gains))
        B = gains[0]  # linear gain
        if np.any(B == 0):
            self.logger.debug("process_minical_calib: process_minical result for gain: {}".format(B))
            # raise "minical failed"
        BC = gains[1]  # linear term of polynomial gain
        CC = gains[2]  # quadratic term of polynomial gain
        # equivalent temperature of noise diode
        Tnd = gains[3]
        # sky, linear gain
        T2 = B * (R2 - R1)
        if np.any(T2 == 0):
            self.logger.debug("process_minical_calib: process_minical result for sky: {}".format(T2))
            # raise "minical failed"
        # sky + ND
        T3 = B * (R3 - R1)
        # load
        T4 = B * (R4 - R1)
        # load + ND
        T5 = B * (R5 - R1)
        Tlinear = [T2, T3, T4, T5]
        T2C = BC * T2 + CC * T2 * T2
        T3C = BC * T3 + CC * T3 * T3
        T4C = BC * T4 + CC * T4 * T4
        T5C = BC * T5 + CC * T5 * T5
        Tquadratic = [T2C, T3C, T4C, T5C]
        # Tsky correction
        FL = T2C / T2
        # non-linearity
        NonLin = 100.0 * (FL - 1.0)
        # Calculate new tsys factors
        tsys_factors = [cal_data['sky'][i] / Tquadratic[0][i] for i in xrange(4)]
        self.logger.info("process_minical_calib: gains: {}".format(gains))
        self.logger.info("process_minical_calib: Tlinear: {}".format(Tlinear))
        self.logger.info("process_minical_calib: Tquadratic: {}".format(Tquadratic))
        self.logger.info("process_minical_calib: Tnd: {}".format(Tnd))
        self.logger.info("process_minical_calib: NonLin: {}".format(NonLin))
        self.logger.info("process_minical_calib: New tsys factors: {}".format(tsys_factors))

        return {'gains':[a.tolist() for a in gains],
                'linear':[a.tolist() for a in Tlinear],
                'quadratic':[a.tolist() for a in Tquadratic],
                'nd-temp':Tnd.tolist(),
                'non-linearity':NonLin.tolist(),
                'tsys-factors':tsys_factors}

    @Pyro4.oneway
    @async.async_method
    def minical_new(self):
        """
        Minical is a procedure that allows us to estimate sky temperature with power meter readout.
        """
        calib = {}
        self._minical_info['running'] = True
        self.logger.info("Performing minical")
        self._wbdc_fe.set_preamp_bias(1, False)
        self._wbdc_fe.set_preamp_bias(2, False)
        msg = "Turning off preamp bias"
        self.logger.debug(msg)
        self.minical_new.cb_updates({'status':msg})
        for i in xrange(1,5):
            self._hppm.set_mode(i,'W')
        calib['mode'] = 'W'
        fe_temp = self._wbdc_fe.read_temp()
        calib['Tload'] = np.array([fe_temp['load1'], fe_temp['load1'], fe_temp['load2'], fe_temp['load2']])
        # collect data
        # self._hppm.zero()
        calib['zero'] = np.array(self._hppm.get_readings(5)) # + self._rad.get_readings()) # {'hppm': self._hppm.get_readings(), "rad": self._rad.get_readings()}
        msg = "Turning on preamp bias"
        self.logger.debug(msg)
        self.minical_new.cb_updates({'status':msg})
        self._wbdc_fe.set_preamp_bias(1, True)
        self._wbdc_fe.set_preamp_bias(2, True)
        msg = "Turning off noise diode and setting feeds to load"
        self.logger.debug(msg)
        self.minical_new.cb_updates({"status": msg})
        self._wbdc_fe.set_feed_state(1, 'load')
        self._wbdc_fe.set_feed_state(2, 'load')
        self._wbdc_fe.set_noise_diode_state(False)
        # self._fe.set_feed(1, 'load')
        # self._fe.set_feed(2, 'load')
        # self._fe.set_ND_state(False) # off
        # collect data, load + no noise diode
        calib['load'] = np.array(self._hppm.get_readings(5)) # + self._rad.get_readings()) #{'hppm': self._hppm.get_readings(), "rad": self._rad.get_readings()}
        msg = "Turning on noise diode and setting feeds to sky"
        self.logger.debug(msg)
        self.minical_new.cb_updates({"status": msg})
        self._wbdc_fe.set_noise_diode_state(True)
        # self._fe.set_ND_state(True)
        # collect data, load + noise diode
        calib['load+ND'] = np.array(self._hppm.get_readings(5))# + self._rad.get_readings())# {'hppm': self._hppm.get_readings(), "rad": self._rad.get_readings()}
        self._wbdc_fe.set_feed_state(1, 'sky')
        self._wbdc_fe.set_feed_state(2, 'sky')
        # time.sleep(10)
        # collect data, sky + noise diode
        calib['sky+ND'] = np.array(self._hppm.get_readings(5))# + self._rad.get_readings()) #{'hppm': self._hppm.get_readings(), "rad": self._rad.get_readings()}
        msg = "Turning off noise diode"
        self.logger.debug(msg)
        self.minical_new.cb_updates({"status":msg})
        self._wbdc_fe.set_noise_diode_state(True)
        # self._fe.set_ND_state(False)
        # collect data, sky + no noise diode
        calib['sky'] = np.array(self._hppm.get_readings(5))# + self._rad.get_readings()) #{'hppm': self._hppm.get_readings(), "rad": self._rad.get_readings()}
        self.logger.debug(calib)
        results = self.process_minical_calib(calib)

        self.minical_new.cb(results)
        self._minical_info['running'] = False


    def get_previous_minical_results(self, filename):
        """
        Given some data file, load in previous minical results.
        This represents a departure from the way previous minical results were reported in the past.
        Before, we used log files to report this information.
        Args:
            filename (str): The name and path of the filename to use.
        Returns:

        """
        pass

    @Pyro4.oneway
    @async.async_method
    def minical(self):
        self._minical_info['running'] = True
        self.logger.info("Performing minical")
        if self.minical.socket_info:
            minical_logger = logging.getLogger(self.logger.name+".MinicalWorker")
            self.minical_worker = LongRunningWorker(self, self.wbdc_fe,
                                                        "perform_minical",
                                                        logger=minical_logger,
                                                        name="MinicalWorker",
                                                        socket_info=self.minical.socket_info,
                                                        cb_info=self.minical.cb_info)
            self.minical_worker.start()
        else:
            results = self.wbdc_fe.perform_minical()
            if not results:
                self.logger.info("Minical failed.")
            else:
                self.logger.info("Minical completed.")
            self.minical.cb(results)
        self._minical_info['running'] = False
    # ==================================================Boresight======================================================

    def calc_bs_points(self, n_points=9, fwhm=None):
        """
        Calculate the points to use for the boresight movement across the sky.
        Args:
            fwhm (float): full width half max. A characteristic of the antenna.
            n_points (int): The number of points for which to calculate boresight.
        Returns:
            list: the boresight points.
        """
        # We compute this for scan length of 2*5*fwhm so that we are enough offsource in both el and xel
        if not fwhm: fwhm = self.apc.k_band_fwhm()
        if n_points < 6 and n_points > 15:
            self.logger.error("calc_bs_points: Boresight for {} points not supported".format(n_points))
            return
        else:
            self.logger.info("calc_bs_points: Calculating {} boresight points".format(n_points))
            points = [-22]
            points.extend(np.linspace(-3, 3, n_points - 2).tolist())
            points.append(22)
            points_scaled = [i*fwhm for i in points]
            self.logger.debug("calc_bs_points: Calculated points: {}".format(points))
            self.logger.debug("calc_bs_points: Calculated scaled points: {}".format(points_scaled))
            return points, points_scaled

    @Pyro4.oneway
    def stop_boresight(self):
        self.logger.info("Stopping boresight.")
        with self.lock:
            self._boresight_info['running'] = False

    @Pyro4.oneway
    @async.async_method
    def boresight(self, el_previous_offsets, xel_previous_offsets,
                        n_points=9, iterations=1, integration_time=2, src_obj=None):
        """
        Perform BS over specified points.

        Boresight is what allows us to ascertain if we're really pointing on source.
        There are the coordinates that the antenna thinks are the correct ones for pointing
        at a source. However, because we're dealing with very high resolution
        (and thus low field of view (FOV)) we have to make darn sure we're actually point
        where we want. By scanning over the sky in the area where we think the source is,
        we hope to establish 'offsets,' or the difference between where the source actually is
        and where the antenna thinks it is. We do this by fitting a gaussian to power meter
        data from a range of points in the viscinity of the source, in both elevation and
        cross elevation.

        This function will run regardless if we're on source or not. Check before.
        Args:
            el_previous_offsets (float): from the el_prog_offsets UI element
            xel_previous_offsets (float): from the xel_prog_offsets UI element
        Keyword Args:
            points (list): The points on which to perform boresight (None)
            iterations (int): The number of boresight iterations to do (1)
            integration_time (int): The number of power meter readings to get at each
                boresight step (2)
            src_obj (tams_source.TAMS_Source): The source object around which we're doing
                boresight
        cb:
            dict:
            'prog': xel and el offset updates
                     (for updating the ui.xel_prog_offsets a ui.el_prog_offsets UI elements.)
            'fit_results': The results from performing a gaussian fit on the xel and el points.
                     See self.gauss_fit for the resulting dict.
        cb_updates:
            dict:
            'status': The current state of the boresight.
        """
        el_previous_offsets = float(el_previous_offsets)
        xel_previous_offsets = float(xel_previous_offsets)
        iterations = int(iterations)
        integration_time = int(integration_time)
        # if self.apc_worker: self.apc_worker.pause()
        # if self.wbdc_pm_worker: self.wbdc_pm_worker.pause()
        self.logger.info("boresight: Using el {} and xel {} as initial offsets".format(el_previous_offsets, xel_previous_offsets))
        self.logger.info("boresight: Doing {} boresight iterations".format(iterations))
        self.logger.info("boresight: Power meter integration time: {}".format(integration_time))
        self.logger.info("boresight: Using {} boresight points".format(n_points))
        self.logger.info("boresight: src_obj: {}".format(src_obj))
        self.logger.info("boresight: type(src_obj): {}".format(type(src_obj)))

        with self.lock:
            self._boresight_info['running'] = True

        points = self.calc_bs_points(n_points=n_points)

        fields = ['on_pol1', 'on_pol2', 'on_minus_off']

        prev_offsets = {'el':{key:el_previous_offsets for key in fields},
                        'xel':{key:xel_previous_offsets for key in fields}}

        delta_offsets = {'el': {key:[] for key in fields},
                         'xel': {key:[] for key in fields}}

        for i_iter in xrange(iterations):
            self.logger.info("Using el {} and xel {} as previous offsets".format(prev_offsets['el'], prev_offsets['xel']))
            timestamp = datetime.datetime.utcnow().strftime("%j-%Hh%Mm%Ss")
            doy = datetime.datetime.utcnow().strftime("%j")
            boresight_dir = "/home/ops/roach_data/sao_test_data/data_dir/boresight_data/{}".format(doy)
            f_name_boresight = "boresight_results_{}.hdf5".format(timestamp)
            if not os.path.exists(boresight_dir):
                try:
                    os.mkdir(boresight_dir)
                    f_name_boresight = os.path.join(boresight_dir, f_name_boresight)
                except:
                    pass
            else:
                f_name_boresight = os.path.join(boresight_dir, f_name_boresight)
            f_boresight = h5py.File(f_name_boresight, 'w')

            if src_obj:
                src_obj = TAMS_Source.fromDict(src_obj)
                src_obj.compute(self._cdscc)
                f_boresight.attrs['name'] = src_obj.name
                f_boresight.attrs['RAJ2000'] = src_obj._ra
                f_boresight.attrs['DECJ2000'] = src_obj._dec
                f_boresight.attrs['az'] = src_obj.az
                f_boresight.attrs['el'] = src_obj.alt
                if isinstance(src_obj.flux, dict):
                    f_boresight.attrs['flux'] = src_obj.flux['K']
                else:
                    f_boresight.attrs['flux'] = src_obj.flux
            else:
                self.logger.warning("boresight: No source object provided")
            pm_callback_args = (integration_time, )
            gauss_results = {"el":{}, "xel":{}} # gaussian fit data

            for direction in ['el', 'xel']:
                self.boresight.cb_updates({'status': 'Calculating offset in {}'.format(direction)})
                dir_points = [i + prev_offsets[direction]['on_minus_off'] for i in points[1]]
                self.logger.info("boresight: Boresight points for given routine in {}: {}".format(direction, dir_points))
                avg_tsys = self.grab_pm_data(direction, dir_points, pm_callback_args)
                if not avg_tsys and not self._boresight_info['running']:
                    self.boresight.cb_updates({'status': "Boresight Cancelled"})
                    self.boresight.cb({})
                    f_boresight.close()
                    return
                group = f_boresight.create_group(direction)
                group.create_dataset('points', data=dir_points)
                group.create_dataset('tsys', data=avg_tsys)

                on_pol1_data = [i[0] for i in avg_tsys]
                on_pol2_data = [i[1] for i in avg_tsys]
                on_minus_off_data = [i[0] - i[2] for i in avg_tsys]

                pm_data = [on_pol1_data, on_pol2_data, on_minus_off_data]

                self.logger.info("boresight: PM readings for boresight scan in {} {}".format(direction, avg_tsys))

                for i in range(len(fields)):
                    data = pm_data[i]
                    field = fields[i]
                    fit = self.gauss_fit(points[0], data, direction)
                    offset_calculated = fit['BS_offset']
                    prog_offset = offset_calculated + prev_offsets[direction][field]

                    prev_offsets[direction][field] = prog_offset
                    gauss_results[direction][field] = fit
                    delta_offsets[direction][field].append(offset_calculated)

                    msg = "Calculated offset in {} for {}: {}".format(direction, field, offset_calculated)
                    self.logger.info("boresight: {}".format(msg))
                    self.boresight.cb_updates({'status': msg})

                # check to see if there is a crazy difference between on_pol1 and on_pol2
                fit_on_pol1 = gauss_results[direction]['on_pol1']
                fit_on_pol2 = gauss_results[direction]['on_pol2']
                mean_on_pol1, mean_on_pol2 = fit_on_pol1['popt'][1], fit_on_pol2['popt'][1]
                sigma_on_pol1, sigma_on_pol2 = fit_on_pol1['popt'][2], fit_on_pol2['popt'][2]

                self.logger.info("boresight: mean for pol1 and pol2 fitting: {:.3f}, {:.3f}".format(mean_on_pol1, mean_on_pol2))
                self.logger.info("boresight: sigma for pol1 and pol2 fitting: {:.3f}, {:.3f}".format(sigma_on_pol1, sigma_on_pol2))
                t, nu = self.welchs_t_test(
                    mean_on_pol1, mean_on_pol2,
                    sigma_on_pol1, sigma_on_pol2,
                    n_points, n_points,
                )
                self.logger.info("boresight: Welch's T-test value for pol1 and pol2 fits: {:.4f}".format(t))
                self.logger.info("boresight: DOF for pol1 and pol2 fits: {:.4f}".format(nu))

                # final_offset = prev_offsets[direction]['on_minus_off']
                final_offset = prev_offsets[direction]['on_pol1']
                setattr(self, "_boresight_offset_{}".format(direction), final_offset)
                self._boresight_info['offset_{}'.format(direction)] = final_offset
                self._apc.set_offset_one_axis(direction, final_offset)
                self.logger.info("boresight: Programmed offset in {} {}".format(direction, prog_offset))


                # gauss_results_dir = self.gauss_fit(avg_tsys, direction)
                # gauss_results_pm1[direction] = gauss_results_dir

                # offset_calculated = gauss_results_dir['BS_offset']
                # delta_offsets[direction].append(offset_calculated)
                # self.logger.info("boresight: calculated offset in {} {}".format(direction, offset_calculated))

                # self.boresight.cb_updates({'status': 'Calculated offset in {}: {}'.format(direction, offset_calculated)})
                # prog_offset = offset_calculated + prev_offsets[direction]
                # set appropriate fields
                # prev_offsets[direction] = prog_offset
                # self._boresight_info['offset_{}'.format(direction)] = prog_offset
                # now actually send the information to the APC
                # self._apc.set_offset_one_axis(direction, prog_offset)

            # self.boresight.cb({'prog': [self._boresight_offset_el, self._boresight_offset_xel],
            #                    'fit_results': [gauss_results['el'], gauss_results['xel']],
            #                    'iter':i_iter+1, 'total_iter':iterations,
            #                    'delta_offsets':delta_offsets})

            self.boresight.cb({
                                "prog": [self._boresight_offset_el, self._boresight_offset_xel],
                                "fit_results": gauss_results,
                                "iter":i_iter+1,
                                "total_iter":iterations,
                                "delta_offsets": delta_offsets,
                                "fields": fields
            })

            f_boresight.close()
        # if self.apc_worker: self.apc_worker.unpause()
        # if self.wbdc_pm_worker: self.wbdc_pm_worker.unpause()

        with self.lock:
            self._boresight_info['running'] = False

    def pm_integrator_all(self, integration_time=2, wait_time=0.02):
        """
        Collect data from all power meters.
        Keyword Arguments:
            integration_time (int/float): Time to integrate for
            wait_time (int/float): The time to wait between grabbing power meter data
        """
        t0 = time.time()
        pm_data = []
        t_initial = time.time()
        t_current = t_initial
        while (t_current - t_initial) < integration_time:
            pm_current = self.wbdc_fe.get_tsys()
            pm_data.append(pm_current['tsys'])
            time.sleep(wait_time)
            # self.logger.debug("Current tsys: {}".format(["{:.3f}".format(i) for i in pm_current['tsys']]))
            t_current = time.time()
        # self.logger.debug("pm_integrator_all: Took {:.2f} seconds to run".format(time.time()-t0))
        integration = np.mean(pm_data, axis=0).tolist()
        self.logger.debug("pm_integrator_all: integration: {}".format(["{:.3f}".format(i) for i in integration]))
        return integration

    def pm_integrator(self, integration_time=2, channel=0):
        """
        Collect power meter data for a certain amount of time (integration_time) on a specified channel
        Keyword Args:
            integration_time (int): The number of times to read the pm data.(2)
            channel (int): Which power meter channel to use for integration. (0)
        Returns:
            float: The average of data from the first power meter.
        """
        pm_data = []
        for i in xrange(integration_time):
            time.sleep(1.0)
            pm_current = self.wbdc_fe.get_tsys()
            pm_data.append(pm_current['tsys'][channel])
            self.logger.debug("Current tsys for channel {}: {:.3f}".format(channel, pm_current['tsys'][channel]))

        return float(sum(pm_data)) / len(pm_data)

    def grab_pm_data(self, axis, locs, pm_integrator_cb_args=None):
        """
        Grab data from power meters at each boresight point.
        Args:
            axis (str): Which axis are we setting? (XEL or EL)
            locs (list): Where to set the offset to.
            pm_callback_dict (dict): A dict containing the following elements:
                'callback': Function to grab data from the WBDC.
                'callback_args': Arguments for the above function.
        Returns:
            list: The average power meter readings from all the boresight points.
        """
        if not pm_integrator_cb_args: pm_integrator_cb_args = ()
        integrated_data = []
        self.logger.info("grab_pm_data: Setting each offset in {}".format(axis))
        self.logger.debug("grab_pm_data: boresight running? {}".format(self._boresight_info['running']))
        for item in locs:
            if self._boresight_info['running']:
                t0 = time.time()
                self._apc.set_offset_one_axis(axis, item)
                self.logger.debug("grab_pm_data: Time changing offset: {:.2f}".format(time.time()-t0))
                t0 = time.time()
                tsys_integration = self.pm_integrator_all(*pm_integrator_cb_args)
                # self.logger.debug("Current Tsys integration: {}".format(["{:.3f}".format(i) for i in tsys_integration]))
                integrated_data.append(tsys_integration)
                self.logger.info("grab_pm_data: Offset in {} set to {}.".format(axis,float(item)))
                try:
                    self.boresight.cb_updates({'status': "Offset in {} set to {:.2f}".format(axis, float(item)),
                                                             'plot_updates': {'axis': axis, 'offset': item, 'tsys': tsys_integration}})
                except AttributeError:
                    self.logger.error("Couldn't call boresight updates callback.")

                self.logger.debug("grab_pm_data: Time getting power meter data: {:.2f}".format(time.time() - t0))
                # print "BS PM averaged reading for sec/step",integrated_data, time.ctime()
            else:
                return None
        return integrated_data

    def gauss_fit(self, points, data, axis_type):
        """
        Fit data through gaussian function
        Args:
            x (list): x values to use for fitting
            data (list): Data to be fit
            axis_type (str): XEL or EL axis
        returns:
            dict:
                'popt': the best fit parameters for the gaussian
                'BS_offset': popt[1]*k_band_fwhm
                'z': x axis elements for plotting
                'gaussian_fn': the gaussian function
                'axis_type': XEL or EL
                'data': The data argument, or the data to be fit.
                'x': The x data corresponding to the 'data' points
        """
        self.logger.debug("gauss_fit: Called.")

        # Gaussian function
        def gauss_function(x, a, x0, sigma, slope=0, intercept=0):
            return a * np.exp(-(x - x0) ** 2 / (2 * sigma ** 2)) + slope * x + intercept

        def line(x, a, b, c, slope, intercept):
            val = slope * x + intercept
            return val

        x = np.array(points)
        y = np.array(data)
        self.logger.info("gauss_fit: Data for boresight: {}".format(y))
        self.logger.debug("gauss_fit: X points for boresight: {}".format(x))
        assert x.shape[0] == y.shape[0], "Data and points must be of equal length"
        mean = 0.0  # beam width
        sigma = 2.1  # beam width
        slope = (y[-1] - y[0]) / (x[-1] - x[0])
        intercept = (y[-1] + y[0]) / 2.
        try:
            self.logger.info(
                "gauss_fit: mean {}, sigma {}, slope {}, intercept {}".format(mean, sigma, slope, intercept))
            popt, pcov = op.curve_fit(gauss_function, x, y, p0=(1, mean, sigma, slope, intercept))
        except Exception as err:
            self.logger.error("gauss_fit: Optimal results could not be obtained for gaussian fit.")
            self.logger.error("gauss_fit: Error: {}".format(err), exc_info=True)
            popt = [0.0 for i in xrange(5)]
            pcov = [0.0 for i in xrange(5)]
        try:
            self.logger.info(
                "gauss_fit: Calculated Amplitude =  {:.3f} +/- {:.3f}".format(popt[0], np.sqrt(pcov[0, 0])))
            self.logger.info("gauss_fit: Calculated Offset = {:.3f} +/- {:.3f}".format(popt[1], np.sqrt(pcov[1, 1])))
            self.logger.info("gauss_fit: Calculated Sigma =  {:.3f} +/- {:.3f}".format(popt[2], np.sqrt(pcov[2, 2])))
        except Exception as err:
            self.logger.error("gauss_fit: Cannot properly display values. Probably performing boresight without having done minical")

        z = np.arange(min(x), max(x), 0.02)
        self.logger.info("gauss_fit: Calculated BS offsets: %f" % (popt[1] * self._apc.k_band_fwhm()))

        return {'popt': list(popt),
                'BS_offset': popt[1] * self._apc.k_band_fwhm(),
                'z': list(z),
                'axis_type': axis_type,
                'x': list(x),
                'data': list(data)}
    # ==================================================== Tipping =====================================================
    @Pyro4.oneway
    @async.async_method
    def tipping(self, save=True):
        """
        Tipping is a routine in which we measure power meter data as we move the antenna from 88 el to 15 el.
        If we're on source, then we need to make sure that we move offsource by setting the offset in one direction
        (xEl) before we perform tipping.
        Keyword Arguments:
            save (bool): Whether or not to save tipping results in an HDF5 file
        cb:
            dict:
            'status': A string telling whether the tipping as successful
            'results': The power meter readings ascending and descending
        cb_updates:
            dict:
            'status': A string telling the current location of the antenna
            'plot_updates': The current power meter readings
            'direction': Whether we're moving up (88) or down (15)
        """
        self._tipping_info['running'] = True
        if self.pm_recording_worker:
            self.pm_recording_worker.pause()
        # first make sure we're offsource
        self.tipping.cb_updates({'status':"Changing offset in XEL to 99 millidegrees"})
        if self.apc.onsource() == 'ONSOURCE':
            self.apc.set_offset_one_axis('XEL', 99)
        self.tipping.cb_updates({'status':"Offset in XEL changed to 99 millidegrees. Performing tipping."})

        results = {}

        timestamp = datetime.datetime.utcnow().strftime("%j-%Hh%Mm%Ss")
        doy = datetime.datetime.utcnow().strftime("%j")
        tipping_dir = "/home/ops/roach_data/sao_test_data/data_dir/tipping_data/{}".format(doy)
        f_name_tipping = "tipping_results_{}.hdf5".format(timestamp)
        if not os.path.exists(tipping_dir):
            try:
                os.mkdir(tipping_dir)
                f_name_tipping = os.path.join(tipping_dir, f_name_tipping)
            except:
                pass
        else:
            f_name_tipping = os.path.join(tipping_dir, f_name_tipping)
        f_tipping = h5py.File(f_name_tipping, 'w')

        for el in [15, 88]:
            if not self._tipping_info['running']:
                self.tipping.cb({'status': "Tipping cancelled"})
                return
            self.logger.info("tipping: Moving to {}".format(el))
            # callback_handler.tipping_updates_callback({'status': 'Moving to {}'.format(el)})
            self.tipping.cb_updates({'status': 'Moving to {}'.format(el)})
            resp = self.apc.move(el, axis='EL')
            self.logger.debug("Response from APC move: {}".format(resp))
            cur_azel = self.apc.get_azel()
            tipping_record_el, tipping_record_tsys = [], []
            while abs(cur_azel['el'] - el) > 2.0:
                if not self._tipping_info['running']:
                    # callback_handler.tipping_callback({'status': "Tipping cancelled"})
                    self.tipping.cb_updates({'status': "Tipping cancelled"})
                    return

                cur_azel = self.apc.get_azel()
                tsys = self.wbdc_fe.get_tsys()

                tipping_record_el.append(cur_azel['el'])
                tipping_record_tsys.append(tsys['tsys'])
                self.tipping.cb_updates({'status': "Current el: {}".format(cur_azel['el']),
                                           'plot_updates': {'el':cur_azel['el'], 'tsys':tsys['tsys']},
                                           'direction': el})


                time.sleep(2.0)
            # can save Python lists in HDF5 datasets.
            grp_el = f_tipping.create_group('el_{}'.format(el))
            grp_el.create_dataset("el", data=tipping_record_el)
            grp_el.create_dataset("tsys", data=tipping_record_tsys)

            result_el = {'tipping_el': tipping_record_el,
                         'tipping_tsys': tipping_record_tsys}

            results[str(el)] = result_el

        f_tipping.close()

        self._tipping_info['running'] = False
        if self.pm_recording_worker:
            self.pm_recording_worker.unpause()
        self.tipping.cb({'status': 'Tipping Complete',
                            'results': results})


    def stop_tipping(self):
        """
        Set the tipping_info 'running' flag to False, which will stop tipping.
        """
        with self.lock:
            self._tipping_info['running'] = False

    # ================================================Pointing on source================================================
    @Pyro4.oneway
    @async.async_method
    def point_onsource(self, src_obj):
        """
        Moving the telescope to point on source can take some time. This function moves the antenna while simultaneously
        feeding informaton back to client about the current antenna position.
        Returns:
            None
        """
        self._pointing_info['running'] = True
        src = TAMS_Source.fromDict(src_obj)
        self._cdscc.date = datetime.datetime.utcnow()
        src.compute(self._cdscc)
        source_name, source_ra, source_dec = src.name, src.ra, src.dec
        long_coord = ephem.hours(str(source_ra))
        lat_coord = ephem.degrees(str(source_dec))
        self.logger.info(
            "point_onsource: {}, RA and DEC (degrees): {}, {}".format(source_name, source_ra, source_dec))
        self.logger.info(
            "point_onsource: {}, RA and DEC (float): {}, {}".format(source_name, np.rad2deg(source_ra), np.rad2deg(source_dec)))
        self.logger.info(
            "point_onsource: {}, RA and DEC (float, old method): {}, {}".format(source_name, str(np.rad2deg(long_coord)), str(np.rad2deg(lat_coord))))
        self.logger.info(
            "point_onsource: {}, Sending {} {} to APC".format(source_name, str(np.rad2deg(source_ra)), str(np.rad2deg(source_dec))))
        response_pnt_src = self.apc.point_source(
                                    'RADEC',
                                     str(np.rad2deg(source_ra)), str(np.rad2deg(source_dec)))
        msg = "Initial response from APC server: {}".format(response_pnt_src)
        self.logger.debug("point_onsource: {}".format(msg))
        self.point_onsource.cb_updates({'status': msg})
        onsource = self.apc.onsource()

        while onsource != "ONSOURCE":

            time.sleep(2.0)
            azel = self.apc.get_azel()
            time.sleep(0.1)
            onsource = self.apc.onsource()
            self.point_onsource.cb_updates({"onsource":onsource,
                                            "azel": azel})

            self.logger.debug("Current az/el: {} {}".format(azel['az'], azel['el']))


        self.point_onsource.cb({"status": "Pointing on source complete."})
        self._pointing_info['running'] = False
    # ================================================Data Recording===================================================

    def create_datafile(self, data_dir, data_file_name=None, source_name="", roach_input=""):
        """
        Set up a data file for recording. This includes data from the APC, ROACHs, and WBDC.
        args:
            - data_dir: The place where the datafile will be saved.
        kwargs:
            - data_file_name: The name of the datafile to be saved.
            - source_name: The name of the observation source.
            - roach_input: The input field from the roachInput1 ui element.
        """
        # timestamp = time.time()
        timestamp = datetime.datetime.utcnow().strftime("%j-%Hh%Mm%Ss")
        if not data_file_name:
            if " " in source_name:
                source_name = source_name.replace(" ","_")
            if roach_input == "":
                data_file_name = "{}.{}.spec.hdf5".format(timestamp, source_name)
            else:
                data_file_name = "{}.{}.{}.spec.hdf5".format(roach_input, timestamp, source_name)

        self.logger.info("create_datafile: Datafile name: {}".format(data_file_name))
        self._data_dir = data_dir
        doy = str(datetime.datetime.utcnow().timetuple().tm_yday)
        project_dir = os.path.join(self._data_dir, doy)
        self._project_dir = project_dir
        data_file_path = os.path.join(project_dir, data_file_name)
        self.logger.info("create_datafile: Datafile path: {}".format(data_file_path))
        # datafile_name = project_dir +'/' +str(self.ui.roachInput1.text())+timestamp+'.'+str(self.ui.source_name_label.text())+ ".spec.h5"
        if not os.path.exists(project_dir):
            self.logger.info("create_datafile: New project directory is being created: {}".format(project_dir))
            os.mkdir(project_dir)
        df = TAMSHDF5File(data_file_path)
        return df

    # def write_data(self, message, df,
    #                scan_duration=0,
    #                n_cycles=0,
    #                current_scan=0,
    #                source_info=None,
    #                initial_accum_number=None):
    #     """
    #     Callback for Spec_Client_Subscriber.
    #     When intialized, this gets called everytime a new accumulation is published to the MessageBus.
    #     args:
    #         - message: The message object from Spec_Client_Subscriber
    #         - df (h5py.File): The datafile h5py object, with datasets already created.
    #     kwargs:
    #         - integrate_time (int): Integration time (5)
    #         - scan_duration (float): How long is the scan going on for (0)
    #         - n_scans (int): the number of scans we're doing.
    #         - current_scan (int): The current scan
    #         - source_info (dict): a dictionary with source info.
    #         - initial_accum_number (dict): The number of accumulations initially.
    #     """
    #     t0 = time.time()
    #     if not source_info: source_info = None
    #     if not initial_accum_number: initial_accum_number = {'sao64k-1': 1, 'sao64k-2': 1, 'sao64k-3': 1, 'sao64k-4': 1}
    #     self.logger.debug("write_data: initial_accum_number: {}".format(initial_accum_number))
    #
    #     roach_name = message[0]
    #     accum_num = message[1] - initial_accum_number[roach_name] - 1
    #     spectrum = message[2]
    #
    #     int_time = self.spec.get_integration(roach_name)
    #     self.accum_nums[roach_name] = accum_num
    #     self.logger.debug("write_data: current max accumulation: {}. Current accumulation number: {}".format(
    #         self.max_accum_num,
    #         accum_num
    #     ))
    #     if accum_num > self.max_accum_num:
    #         # now we resize everything.
    #         self.max_accum_num = accum_num
    #         t0 = time.time()
    #         # resize the datasets to match the number of accumulations
    #         for key in df.keys():
    #             if str(key) == 'pol':
    #                 continue
    #             else:
    #                 df[key].resize(accum_num + 1, axis=0)
    #         # self.logger.debug("write_data: Took {:.5f} seconds to resize".format(time.time() - t0))
    #         # time info
    #         timestamp = int(time.time())
    #         date_obs = str(datetime.datetime.now().date())
    #         time_obs = str(datetime.datetime.now().time())
    #
    #         # wbdc stuff
    #         t0 = time.time()
    #         # pm_dict = self.wbdc_fe.get_tsys()# get power meter info
    #         # tsys = pm_dict['tsys']
    #
    #         with self.lock:
    #             tsys = self.tsys_info['tsys']
    #
    #         # self.logger.debug("write_data: Took {:.5f} seconds to get WBDC data".format(time.time() - t0))
    #
    #         # apc stuff
    #         t0 = time.time()
    #         # current_azel = self.apc.get_azel()
    #         # offsets = self.apc.get_offsets()
    #         # offsets_array = [offsets['el'], offsets['xel']]
    #         # onsource = source_info['status']
    #
    #         with self.lock:
    #             current_azel = self.apc_info['azel']
    #             offsets = self.apc_info['offsets']
    #             onsource = self.apc_info['onsource']
    #
    #         # self.logger.debug
    #         # self.logger.debug("write_data: Took {:.5f} seconds to get antenna data".format(time.time() - t0))
    #
    #         if (onsource == "ONSOURCE"):
    #             onsource = 1
    #         else:
    #             onsource = 0
    #
    #         t0 = time.time()
    #         bandwidth = [1020 for i in xrange(4)]
    #         lst = str(self._cdscc.sidereal_time())
    #         # SR_state = 'mid'
    #         # now we start writing the information to the datafile
    #         # self.logger.debug("write_data: Writing to spot {} in datafile".format(accum_num))
    #         df['timestamp'][accum_num] = timestamp
    #         df['onsource'][accum_num] = onsource
    #         df['current_azel'][accum_num] = [current_azel['az'], current_azel['el']]
    #         df['offsets'][accum_num] = [offsets['el'], offsets['xel']]
    #
    #         df['mode'][accum_num] = ['A1P1', 'A2P1', 'A1P2', 'A2P2']
    #         df['source_name'][accum_num] = source_info['name']
    #         df['source_radec'][accum_num] = [str(source_info['ra']), str(source_info['dec'])]
    #         df['source_long_lat'][accum_num] = [str(source_info['long']), str(source_info['lat'])]
    #         df['source_azel'][accum_num] = [str(source_info['az']), str(source_info['el'])]
    #
    #         df['Tsys'][accum_num] = tsys
    #         df['scan_number'][accum_num] = current_scan
    #         df['scan_duration'][accum_num] = scan_duration
    #         df['LST'][accum_num] = lst
    #         df['NSCANS'][0] = 2 * n_cycles
    #         df['integ_time'][0] = int_time
    #         df['date_obs'][accum_num] = date_obs
    #         df['time_obs'][accum_num] = time_obs
    #         df['bandwidth'][accum_num] = bandwidth
    #         df['v_ref'][0] = source_info['ref_frame']
    #         df['vsys'][0] = source_info['vel']
    #         df['rest_freq'][0] = source_info['rest_freq']
    #         df['obs_freq'][0] = source_info['observing_freq']
    #         # self.logger.debug("write_data: Took {:.5f} seconds to write data".format(time.time() - t0))
    #
    #     else:
    #         pass
    #
    #     # Now write the spectrum
    #     try:
    #         t0 = time.time()
    #         df[self.table_corr[roach_name]][accum_num] = np.asarray(spectrum, dtype=float)
    #         # self.logger.debug("write_data: Took {:.5f} seconds to write accumulation".format(time.time() - t0))
    #
    #     except (ValueError, KeyError) as err:
    #         self.logger.error(
    #             "Couldn't write spectrum {} for roach name {}. Current max accum is {}".format(accum_num, roach_name,
    #                                                                                            self.max_accum_num))
    #         self.logger.error(
    #             "Current size of {}'s spectrum dataset: {}".format(roach_name, df[self.table_corr[roach_name]][...].shape))
    #         self.logger.error("Error: {}".format(err))
    #
    #     df.flush()

    def write_data(self, message, df,
                   scan_duration=0,
                   n_cycles=0,
                   current_scan=0,
                   source_info=None,
                   initial_accum_number=None):
        """
        When intialized, this gets called everytime a new accumulation is published to the MessageBus.
        args:
            - message: The message object from Spec_Client_Subscriber
            - df (h5py.File): The datafile h5py object, with datasets already created.
        kwargs:
            - integrate_time (int): Integration time (5)
            - scan_duration (float): How long is the scan going on for (0)
            - n_scans (int): the number of scans we're doing.
            - current_scan (int): The current scan
            - source_info (dict): a dictionary with source info.
            - initial_accum_number (dict): The number of accumulations initially.
        """
        t0 = time.time()
        if not source_info: source_info = None
        if not initial_accum_number: initial_accum_number = {'sao64k-1': 1, 'sao64k-2': 1, 'sao64k-3': 1, 'sao64k-4': 1}
        self.logger.debug("write_data: Called.")

        roach_name = message[0]
        accum_num = message[1] #- initial_accum_number[roach_name] - 1
        spectrum = message[2]
        self.accum_nums[roach_name] = accum_num
        int_time = self.spec.get_integration(roach_name)
        cur_size = df[self.table_corr[roach_name]].shape[0]
        df[self.table_corr[roach_name]].resize(cur_size + 1, axis=0)
        # Now write the spectrum
        df[self.table_corr[roach_name]][cur_size-1] = np.asarray(spectrum, dtype=float)

        self.logger.debug("write_data: current max accumulation: {}. Current accumulation number: {}. Current size of roach {} dataset: {}" .format(
            self.max_accum_num,
            accum_num,
            roach_name,
            cur_size
        ))

        if cur_size + 1 > self.max_accum_num:
            self.logger.debug("write_data: Updating non spectra fields")
            self.max_accum_num = cur_size + 1
            # now we resize everything.
            t0 = time.time()
            # resize the datasets to match the number of accumulations
            for key in df.keys():
                if str(key) in ["pol", "spectraCh1", "spectraCh2", "spectraCh3", "spectraCh4"]:
                    continue
                else:
                    df[key].resize(cur_size + 1, axis=0)
            # self.logger.debug("write_data: Took {:.5f} seconds to resize".format(time.time() - t0))
            # time info
            timestamp = int(time.time())
            date_obs = str(datetime.datetime.now().date())
            time_obs = str(datetime.datetime.now().time())

            # wbdc stuff
            t0 = time.time()
            # pm_dict = self.wbdc_fe.get_tsys()# get power meter info
            # tsys = pm_dict['tsys']

            with self.lock:
                tsys = self.tsys_info['tsys']

            # self.logger.debug("write_data: Took {:.5f} seconds to get WBDC data".format(time.time() - t0))

            # apc stuff
            t0 = time.time()
            # current_azel = self.apc.get_azel()
            # offsets = self.apc.get_offsets()
            # offsets_array = [offsets['el'], offsets['xel']]
            # onsource = source_info['status']

            with self.lock:
                current_azel = self.apc_info['azel']
                offsets = self.apc_info['offsets']
                onsource = self.apc_info['onsource']

            # self.logger.debug
            # self.logger.debug("write_data: Took {:.5f} seconds to get antenna data".format(time.time() - t0))

            if (onsource == "ONSOURCE"):
                onsource = 1
            else:
                onsource = 0

            t0 = time.time()
            bandwidth = [1020 for i in xrange(4)]
            lst = str(self._cdscc.sidereal_time())
            # SR_state = 'mid'
            # now we start writing the information to the datafile
            # self.logger.debug("write_data: Writing to spot {} in datafile".format(accum_num))
            df['timestamp'][cur_size-1] = timestamp
            df['onsource'][cur_size-1] = onsource
            df['current_azel'][cur_size-1] = [current_azel['az'], current_azel['el']]
            df['offsets'][cur_size-1] = [offsets['el'], offsets['xel']]

            df['mode'][cur_size-1] = ['A1P1', 'A2P1', 'A1P2', 'A2P2']
            df['source_name'][cur_size-1] = source_info['name']
            df['source_radec'][cur_size-1] = [str(source_info['ra']), str(source_info['dec'])]
            df['source_long_lat'][cur_size-1] = [str(source_info['long']), str(source_info['lat'])]
            df['source_azel'][cur_size-1] = [str(source_info['az']), str(source_info['el'])]

            df['Tsys'][cur_size-1] = tsys
            df['scan_number'][cur_size-1] = current_scan
            df['scan_duration'][cur_size-1] = scan_duration
            df['LST'][cur_size-1] = lst
            df['NSCANS'][0] = 2 * n_cycles
            df['integ_time'][0] = int_time
            df['date_obs'][cur_size-1] = date_obs
            df['time_obs'][cur_size-1] = time_obs
            df['bandwidth'][cur_size-1] = bandwidth
            df['v_ref'][0] = source_info['ref_frame']
            df['vsys'][0] = source_info['vel']
            df['rest_freq'][0] = source_info['rest_freq']
            df['obs_freq'][0] = source_info['observing_freq']
            # self.logger.debug("write_data: Took {:.5f} seconds to write data".format(time.time() - t0))
        else:
            pass

        df.flush()


    def get_source_info(self, src_obj):
        """
        Get source information for writing data files.
        Args:
            src_name (str): The name of the source.
            src_obj (tams_source.TAMS_Source):
                The source object, containing information about the location in the sky.
        Returns:
            dict: Lots of information relevant to writing data files.
        """
        source_rest_freq = 2.223508e10
        # self._cdscc.date = datetime.datetime.utcnow()
        src_obj = TAMS_Source.fromDict(src_obj)
        src_obj.compute(self._cdscc)
        # status = self._apc.onsource()
        # source_azel = self._apc.get_azel()
        # source_az = source_azel['az']
        # source_el = source_azel['el']
        source_coord = 'J2000'
        source_ref_frame = 'OPTI-LSR'
        observing_freq = source_rest_freq / (1. + (float(src_obj.velo) / (constants.c / 1000)))

        return_val = {'name': str(src_obj.name), 'ra': str(src_obj.ra),
                      'dec': str(src_obj.dec), 'long': src_obj.get_long(),
                      'lat': src_obj.get_lat(), 'vel': float(src_obj.velo),
                      'rest_freq': float(source_rest_freq),
                      'az': str(src_obj.az), 'el': str(src_obj.alt),
                      'coord': source_coord, "ref_frame": source_ref_frame,
                      "observing_freq": float(observing_freq)}
                      # "status": status}

        return return_val

    @Pyro4.oneway
    @Pyro4.callback
    def spectrometer_callback(self, data):
        """
        Callback for the data acquisition subscriber.
        Args:
            message (list): (roach_name, n_accumulations, spectrum)
            df (h5py.File): The h5py file
            initial_accum_number (dict): The initial number of accumulations in the ROACH registers
            n_cycles (int): The number of scans to complete
            time_per_scan (float): The time in seconds to spend on each scan.
            src_obj (TAMSSource): The source TAMS_Source object
        Returns:
            None
        """
        self.logger.debug("spectrometer_callback: Called.")
        self.logger.debug("spectrometer_callback: Number of keys in data: {}".format(len(list(data.keys()))))
        n_cycles = self._data_acquisition_info['n_cycles']
        time_per_scan = self._data_acquisition_info['tperscan']
        src_obj = self._data_acquisition_info['src_obj']
        initial_accum_number = self._data_acquisition_info['initial_accum_number']
        scan_duration = self._data_acquisition_info['scan_duration']
        source_info = self.get_source_info(src_obj)
        current_scan = self.get_current_scan()

        message = [data['name'], data['accum_number'], data['accum']]
        if self.df:
            self.write_data(message, self.df,
                            scan_duration=scan_duration,
                            current_scan=current_scan,
                            n_cycles=n_cycles,
                            source_info=source_info,
                            initial_accum_number=initial_accum_number)
        else:
            self.logger.debug("spectrometer_callback: Nothing to write to.")

    #
    # @Pyro4.oneway
    # @Pyro4.callback
    # def spectrometer_callback(self, message,
    #                         initial_accum_number,
    #                         src_obj, n_cycles, time_per_scan):
    #     """
    #     Callback for the data acquisition subscriber.
    #     Args:
    #         message (list): (roach_name, n_accumulations, spectrum)
    #         df (h5py.File): The h5py file
    #         initial_accum_number (dict): The initial number of accumulations in the ROACH registers
    #         n_cycles (int): The number of scans to complete
    #         time_per_scan (float): The time in seconds to spend on each scan.
    #         src_obj (TAMSSource): The source TAMS_Source object
    #     Returns:
    #         None
    #     """
    #     # self.logger.debug("subscriber_callback: Called. Roach name: {}, accumulation {}, timestamp {}".format(message[0], message[1], message[3]))
    #     scan_duration = 2 * n_cycles * time_per_scan
    #
    #     source_info = self.get_source_info(src_obj)
    #     current_scan = self.get_current_scan()
    #
    #     if self.df:
    #         self.write_data(message, self.df,
    #                         scan_duration=scan_duration,
    #                         current_scan=current_scan,
    #                         n_cycles=n_cycles,
    #                         source_info=source_info,
    #                         initial_accum_number=initial_accum_number)
    #     else:
    #         self.logger.debug("Nothing to write to.")


    @Pyro4.oneway
    @async.async_method
    def record_data(self,
                    ROACH_integration_time=5,
                    n_cycles=2,
                    time_per_scan=10,
                    src_obj=None,
                    roach_input="",
                    two_beam_worker_cb_info=None):
        """
        Record data for a set number of scans, each lasting a specific amount of time.
        Keyword Args:
            ROACH_integration_time (float): The amount of time to integrate the ROACHes for.
            n_cycles (int): The number of scans to complete
            time_per_scan (float): The time in seconds to spend on each scan.
            src_obj (TAMS_Source): The source TAMS_Source object
        Returns:
        """
        self.logger.debug("record_data: Called.")
        self.logger.info("record_data: self.logger.level: {}".format(self.logger.level))
        ROACH_integration_time = float(ROACH_integration_time)
        n_cycles = int(n_cycles)
        time_per_scan = float(time_per_scan)

        if not two_beam_worker_cb_info:
            two_beam_worker_cb_info={"cb_handler":self.record_data.cb_handler,
                                     "cb":self.record_data.cb_name,
                                     "cb_updates":self.record_data.cb_updates_name}

        self._data_acquisition_info = {'running':True,
                                       'tperscan':time_per_scan,
                                       'n_cycles':n_cycles,
                                       'scan_duration': 2 * n_cycles * time_per_scan,
                                       'src_obj':src_obj}
        self.max_accum_num = -1
        self.accum_nums = {'sao64k-1': 0, 'sao64k-2': 0, 'sao64k-3': 0, 'sao64k-4': 0}
        self.table_corr = {'sao64k-{}'.format(i):"spectraCh{}".format(i) for i in xrange(1,5)}

        # initial_accum_number = {name: self._spec.get_cur_accum_number(name) for name in self._spec.get_roach_names()}

        src_obj = TAMS_Source.fromDict(src_obj)
        src_obj.compute(self._cdscc)
        src_name = src_obj.name

        # if not self._spec.publishing_started:
        #     self._spec.start_publishing()

        self.logger.debug("Starting the two beam worker. Tperscan: {}, n cycles: {}, source name: {}".format(
            time_per_scan,
            n_cycles,
            src_name
        ))

        self.logger.debug("cb: {}, cb_updates: {}".format(self.record_data.cb.__name__, self.record_data.cb_updates.__name__))
        self.logger.debug("Type cb: {}, Type cb_updates: {}".format(type(self.record_data.cb), type(self.record_data.cb_updates)))

        # initialize the apc_info and tsys_info attributes
        timestamp = datetime.datetime.utcnow()
        onsource = self.apc.onsource()
        azel = self.apc.get_azel()
        offsets = self.apc.get_offsets()

        self.apc_info = {'timestamp': timestamp.isoformat(),
                         'azel':azel,
                         'offsets':offsets,
                         'onsource':onsource}

        tsys = self.wbdc_fe.get_tsys()

        self.tsys_info = {'timestamp': timestamp.isoformat(),
                         'tsys': tsys['tsys'],
                         'pm_readings': tsys['pm_readings']}

        if not self.pm_recording_worker:
            pm_recording_logger = logging.getLogger("{}.{}".format(self.logger.name, "WBDCRecordingWorker"))
            self.pm_recording_worker = PowerMeterWorker(self, self.wbdc_fe, ROACH_integration_time,
                                                          logger=pm_recording_logger, loglevel=logging.DEBUG)
            self.pm_recording_worker.start()
        else:
            self.pm_recording_worker.set_update_time(ROACH_integration_time)

        if not self.apc_recording_worker:
            apc_recording_logger = logging.getLogger("{}.{}".format(self.logger.name, "APCRecordingWorker"))
            self.apc_recording_worker = APCWorker(self, self.apc, ROACH_integration_time,
                                                  logger=apc_recording_logger, loglevel=logging.DEBUG)
            self.apc_recording_worker.start()
        else:
            self.apc_recording_worker.set_update_time(ROACH_integration_time)

        twobeam_logger = logging.getLogger("{}.{}".format(self.logger.name, "TwoBeamNodWorker"))
        init_el, init_xel = self._boresight_offset_el, self._boresight_offset_xel
        self.logger.debug("Initial offsets for two beam worker: {}, {}".format(init_el, init_xel))
        self.twobeamworker = TwoBeamNodWorker(self,self._apc, n_cycles, time_per_scan, src_obj,
                                              init_el, init_xel,
                                              logger=twobeam_logger, loglevel=logging.DEBUG,
                                              cb_info=two_beam_worker_cb_info,
                                              socket_info=self.record_data.socket_info)
        self.twobeamworker.start()
        self.logger.debug("Two beam worker started.")
        time.sleep(1.0)

        self.logger.debug("record_data: Creating data file...")
        df = self.create_datafile(self._data_dir,
                                  data_file_name=None,
                                  source_name=src_name,
                                  roach_input=roach_input)
        self.df = df
        self.logger.debug("record_data: Data file created.")

        self.spec.set_integrations(ROACH_integration_time)
        time.sleep(2.0)
        # initial_accum_number = {name: self._spec.get_cur_accum_number(name) for name in self._spec.get_roach_names()}
        initial_accum_number = {name: 1 for name in self._spec.get_roach_names()}
        self._data_acquisition_info['initial_accum_number'] = initial_accum_number
        self.logger.debug("record_data: Initial accumulation: {}".format(initial_accum_number))
        # initial_accum_number = None
        if not self.running():
            # If we're not registered on a Pyro4.Daemon already, then we have to do it ourselves, and register
            # the current object on said Daemon.
            self.logger.debug("This object currently not registered on the nameserver. Doing so now.")
            self.internal_server_thread = threading.Thread(target=self.launch_server, kwargs={"ns_port":self._ns_tunnel.ns_port,"threaded":True})
            self.internal_server_thread.daemon = True
            self.internal_server_thread.start()
            time.sleep(2.0)

        # self.spec.start_publishing(self, initial_accum_number, src_obj.toDict(), n_cycles, time_per_scan)
        self.spec.start_publishing(continuous_record=False, cb_info={"cb_handler":self,
                                                            "cb":"spectrometer_callback"})
        self.logger.info("record_data: Starting to write data!")
        # self.logger.debug("Two Beam worker status: {}".format(self.twobeamworker.running()))
        # while self.twobeamworker.running():
        #     time.sleep(1.0)

    @Pyro4.oneway
    @async.async_method
    def interrupt_recording(self):
        """
        Stop data acquisition. This will generally come in the middle of an observation.
        """
        self.logger.info("Stopping recording data.")
        self.twobeamworker.stop()
        counter = 0
        while self.twobeamworker.running():
            self.logger.debug("Waiting for nodding to stop. Counter {}".format(counter))
            counter += 1
            time.sleep(3)
        self.logger.info("Nodding stopped.")

        try:
            self.stop_nodding()
        except Exception as err:
            self.logger.error("Couldn't call stop_nodding: {}".format(err), exc_info=True)

    def stop_nodding(self):
        """
        Stop nodding routine.
        This means stopping apc_recording_worker, pm_recording_worker, and spectra publishing
        """
        self.logger.debug("Pausing then stopping publishing.")
        self._spec.pause_publishing()
        self._spec.stop_publishing()
        time.sleep(5)
        self.logger.debug("Closing the datafile")
        if self.df:
            self.df.close()
        self.df = None
        self.logger.debug("Datafile closed.")
        # self.apc_recording_worker.pause()
        # self.apc_recording_worker.stop()
        # self.apc_recording_worker = None
        #
        # self.pm_recording_worker.pause()
        # self.pm_recording_worker.stop()
        # self.pm_recording_worker = None

        if self._spec.publishing_started:
            error_msg = "Was not able to stop publishing. Did not stop nodding routine properly"
            self.logger.error(error_msg)
            self.record_data.cb_updates({
                                         'status':error_msg,
                                         'feed_status': 0,
                                         'scan': -1
            })
            self.record_data.cb()
            self._data_acquisition_info['running'] = False
            raise RuntimeError("Was not able to stop publishing spectra. This is a known bug. Please report.")
        else:
            self.logger.debug("Data acquisition finished.")
            self.record_data.cb_updates({'status': "Datafile closed, observing finished.",
                                                       'feed_status': 0,
                                                       'scan': -1})
            self.record_data.cb()
            self._data_acquisition_info['running'] = False

    def get_current_scan(self):
        """
        Get the current scan in the two beam nodding worker in a thread safe manner.
        If the two beam nodding worker doesn't exist, then we raise an error.
        Returns:
            int: The current scan number, according to the two beam nodding worker
        """
        if self.twobeamworker:
            with self.lock:
                return self.twobeamworker.get_scan()
        else:
            raise RuntimeError("No two beam nodding thread currently instantiated.")

    # ================================ WBDC/FE Callbacks ================================

    @Pyro4.oneway
    @async.async_method
    def start_pm_publishing(self, update_rate=2, worker_cb_info=None):
        """Start the pm worker"""
        self.logger.info("Starting PM publishing!")
        if self.pm_recording_worker:
            self.logger.debug("Stopping existing PM publisher")
            self.pm_recording_worker.pause()
            self.pm_recording_worker.stop()
            self.pm_recording_worker = None
        logger = logging.getLogger(self.logger.name + ".PMWorker")
        logger.setLevel(logging.INFO)
        self.pm_recording_worker = PowerMeterWorker(self, self.wbdc_fe,
                                                    update_rate,
                                                    cb_info=worker_cb_info,
                                                    logger= logger,
                                                    socket_info=self.start_pm_publishing.socket_info)

        # self.pm_recording_worker._async.async_method = True
        self.pm_recording_worker.start()
        self.start_pm_publishing.cb()

    @Pyro4.oneway
    @async.async_method
    def pause_pm_publishing(self):
        """Pause the pm publishing"""
        if self.pm_recording_worker:
            self.pm_recording_worker.pause()
        self.pause_pm_publishing.cb()

    @Pyro4.oneway
    @async.async_method
    def stop_pm_publishing(self):
        """Stop the pm publishing"""
        if self.pm_recording_worker:
            self.pm_recording_worker.pause()
            self.pm_recording_worker.stop()
            self.pm_recording_worker = None
        self.stop_pm_publishing.cb()

    @Pyro4.oneway
    @async.async_method
    def get_pm_attens(self):
        """Get WBDC attenuation"""
        try:
            results = self.wbdc_fe.get_pm_attens()
            msg = "get_pm_attens: Success"
            self.logger.debug(msg)
        except Exception as err:
            results = None
            msg = "get_pm_attens: Failed: {}".format(err)
            self.logger.error(msg)
        self.get_pm_attens.cb({'status': msg,
                               'results': results})

    @Pyro4.oneway
    @async.async_method
    def get_tsys(self):
        try:
            results = self.wbdc_fe.get_tsys()
            self.get_tsys.cb(results)
        except Exception as e:
            msg = "get_tsys failed with error: {}".format(e)
            self.logger.error(msg, exc_info=True)
            self.get_tsys.cb({"status":msg})

    @Pyro4.oneway
    @async.async_method
    def get_tsys_factors(self):
        """Get tsys factors from WBDC"""
        try:
            factors = self.wbdc_fe.get_tsys_factors()
            self.get_tsys_factors.cb(factors)
        except Exception as e:
            msg = "get_tsys_factors failed with error: {}".format(e)
            self.logger.error(msg)
            self.get_tsys_factors.cb({"status":msg})

    @Pyro4.oneway
    @async.async_method
    def get_WBDCFrontEnd_state(self):
        """get the WBDC Front End state, asynchronously"""
        try:
            results = self.wbdc_fe.get_WBDCFrontEnd_state()
            self.logger.info(results)
            self.get_WBDCFrontEnd_state.cb(results)
        except Exception as e:
            msg = "get_WBDCFrontEnd_state failed with error: {}".format(e)
            self.logger.error(msg, exc_info=True)
            self.get_WBDCFrontEnd_state.cb({"status":msg})

    # ================================ APC Callbacks ================================
    @Pyro4.oneway
    @async.async_method
    def start_apc_publishing(self, update_rate=2, worker_cb_info=None):
        """Start the apc worker"""
        self.logger.info("Starting APC publishing!")
        if self.apc_recording_worker:
            self.logger.debug("Stopping existing APC publisher")
            self.apc_recording_worker.pause()
            self.apc_recording_worker.stop()
            self.apc_recording_worker = None
        logger = logging.getLogger(self.logger.name + ".APCWorker")
        logger.setLevel(logging.INFO)

        self.apc_recording_worker = APCWorker(self, self.apc, update_rate,
                                        logger=logger,
                                        cb_info=worker_cb_info,
                                         socket_info=self.start_apc_publishing.socket_info)
        # self.apc_recording_worker._async.async_method = True
        self.apc_recording_worker.start()
        self.start_apc_publishing.cb()

    @Pyro4.oneway
    @async.async_method
    def pause_apc_publishing(self):
        """Pause the apc publishing"""
        if self.apc_recording_worker:
            self.apc_recording_worker.pause()
        self.pause_apc_publishing.cb()

    @Pyro4.oneway
    @async.async_method
    def stop_apc_publishing(self):
        """Stop the apc publishing"""
        if self.apc_recording_worker:
            self.apc_recording_worker.pause()
            self.apc_recording_worker.stop()
            self.apc_recording_worker = None
        self.stop_apc_publishing.cb()

    @Pyro4.oneway
    @async.async_method
    def get_azel(self):
        """Call APC get_azel asynchronously"""
        # self.logger.debug("get_azel: Called.")
        results = self.apc.get_azel()
        for i in range(10):
            self.get_azel.cb_updates({i:i})
        # self.logger.debug("get_azel: azel: {}".format(results))
        # self.logger.debug(self.get_azel.cb)
        self.get_azel.cb(results)
        self.logger.debug("get_azel: Cb called.")


    @Pyro4.oneway
    @async.async_method
    def set_offset_el(self, value):
        """Set EL offset asynchronously"""
        self.logger.debug("Setting el offset to {}".format(value))
        self._boresight_offset_el = value
        resp = self.apc.set_offset_one_axis('EL', value)
        self.logger.debug("Response from APC server {}".format(resp))
        self.set_offset_el.cb(resp)

    @Pyro4.oneway
    @async.async_method
    def set_offset_xel(self, value):
        """Set xEL offset asynchronously"""
        self.logger.debug("Setting xel offset to {}".format(value))
        self._boresight_offset_xel = value
        resp = self.apc.set_offset_one_axis('XEL', value)
        self.logger.debug("Response from APC server {}".format(resp))
        self.set_offset_xel.cb(resp)

    @Pyro4.oneway
    @async.async_method
    def get_offsets(self):
        """
        Call the APC get_offsets method asynchronously
        """
        results = self.apc.get_offsets()
        self.get_offsets.cb(results)

    @Pyro4.oneway
    @async.async_method
    def onsource(self):
        """
        Call the APC onsource method asynchronously
        """
        results = self.apc.onsource()
        self.onsource.cb(results)

    # ================================ Spectrometer Callbacks ================================
    @Pyro4.oneway
    @async.async_method
    def start_rms_publishing(self, update_rate=31.19, worker_cb_info=None):
        """Start the RMS worker"""
        self.logger.info("Starting RMS publishing!")
        if self.rms_recording_worker:
            self.logger.debug("Stopping existing RMS publisher")
            self.rms_recording_worker.pause()
            self.rms_recording_worker.stop()
            self.rms_recording_worker = None
        self.logger.debug("worker_cb_info: {}".format(worker_cb_info))
        logger = logging.getLogger(self.logger.name + ".RMSWorker")
        logger.setLevel(logging.INFO)

        self.rms_recording_worker = RMSWorker(self, self.spec, update_rate,
                                    logger=logger, cb_info=worker_cb_info, socket_info=self.start_rms_publishing.socket_info)
        # self.rms_recording_worker._async.async_method = True
        self.rms_recording_worker.start()
        self.start_rms_publishing.cb()

    @Pyro4.oneway
    @async.async_method
    def pause_rms_publishing(self):
        """Pause the apc publishing"""
        if self.rms_recording_worker:
            self.rms_recording_worker.pause()
        self.pause_rms_publishing.cb()

    @Pyro4.oneway
    @async.async_method
    def stop_rms_publishing(self):
        """Stop the apc publishing"""
        if self.rms_recording_worker:
            self.rms_recording_worker.pause()
            self.rms_recording_worker.stop()
            self.rms_recording_worker = None
        self.stop_rms_publishing.cb()

    @Pyro4.oneway
    @async.async_method
    def get_current_accum_all(self):
        """Get the current accumulations for all the ROACHs"""
        self.logger.debug("get_current_accum_all: Called.")
        spectra = [self.spec.get_cur_accum(i) for i in xrange(1,5)]
        self.get_current_accum_all.cb({"success":True, "spectra":spectra})

    @Pyro4.oneway
    @async.async_method
    def get_current_accum(self, i):
        """Get the current accumulation in a specific ROACH"""
        self.logger.debug("get_current_accum: Called.")
        spectrum = self.spec.get_cur_accum(i)
        self.get_current_accum.cb({"success":True, "spectrum":[i,spectrum]})

    def save_fits(self):
        """
        Convert the current data file to FITS format.
        """
        try:
            if self.df:
                self.logger.debug("Creating FITS file")
                self.df.flush()
                self.df.convert_to_gbtidlfits()
                self.logger.debug("FITS file created.")
        except Exception as err:
            self.logger.error("Couldn't create FITS file. Error: {}".format(err), exc_info=True)

    @Pyro4.oneway
    @async.async_method
    def get_adc_gains(self, **kwargs):
        """Get ADC gains"""
        try:
            gains = self.spec.get_adc_gains(**kwargs)
            status = "Successfully got ADC gains"
            self.logger.info(status)
        except Exception as err:
            gains = None
            status = "Getting ADC gains failed: {}".format(err)
            self.logger.error(status)
        self.get_adc_gains.cb({"status": status, "gains":gains})

    @Pyro4.oneway
    @async.async_method
    def calc_rms(self, i):
        """Calculate ADC RMS for a specific ROACH"""
        try:
            rms = self.spec.calc_rms(i)
            status = "New RMS for ROACH {}: {}".format(i, rms)
            self.logger.info(status)
        except Exception as err:
            rms = None
            status = "Calculating RMS for ROACH {} failed: {}".format(i, err)
            self.logger.error(err)
        self.calc_rms.cb({"status":status, "rms":[i,rms]})

    @Pyro4.oneway
    @async.async_method
    def set_adc_gain(self, i, val):
        """Set ADC gain for a specific ROACH"""
        val = float(val)
        try:
            self.spec.set_adc_gain(i, val)
            status = "Successfully set ADC gain for ROACH {} to {}".format(i, val)
            self.logger.info(status)
        except Exception as err:
            status = "Setting gain for ADC gain for ROACH {} failed: {}".format(i, err)
            self.logger.error(err)
        self.set_adc_gain.cb({"status":status,"gain":[i,val]})

    @Pyro4.oneway
    @async.async_method
    def calibrate_adc_all(self):
        """Calibrate all the ADCs"""
        results = {}
        for roach in xrange(1,5):
            try:
                self.spec.calibrate(roach)
                results[roach] = {"success":True}
                self.logger.info("calibrate_adc_all: ROACH {} calibration successful.".format(roach))
            except Exception as e:
                results[roach] = {"success":False, "error":e}
                self.logger.error("calibrate_adc_all: ROACH {} calibration failed: {}".format(roach, e))
        self.calibrate_adc_all.cb(results)

    @Pyro4.oneway
    @async.async_method
    def initialize_adc_all(self):
        """Initialize all the ADCs"""
        results = {}
        for roach in xrange(1,5):
            try:
                self.spec.initialize(roach)
                results[roach] = {"success":True}
                self.logger.info("initialize_adc_all: ROACH {} initializaton successful.".format(roach))
            except Exception as e:
                results[roach] = {"success":False, "error":e}
                self.logger.error("initialize_adc_all: ROACH {} initialization failed: {}".format(roach, e))
        self.initialize_adc_all.cb(results)

    @Pyro4.oneway
    @async.async_method
    def set_fft_shift_all(self):
        """Set the FFT shift for all FPGAs to 63"""
        results = {}
        fft_shift_val = int(0b0000000000111111)
        for roach in xrange(1,5):
            try:
                self.spec.fft_shift_set(roach, fft_shift_val)
                results[roach] = {"success":True}
                self.logger.info("set_fft_shift_all: ROACH {} FFT shift set successfully.".format(roach))
            except Exception as e:
                results[roach] = {"success":False, "error":e}
                self.logger.error("set_fft_shift_all: ROACH {} failed to set FFT shift: {}".format(roach, e))
        self.set_fft_shift_all.cb(results)

    @Pyro4.oneway
    @async.async_method
    def sync_start_all(self):
        """Synchronize vector accumulators for all FPGAs"""
        results = {}
        for roach in xrange(1,5):
            try:
                self.spec.sync_start(roach)
                results[roach] = {"success":True}
                self.logger.info("sync_start_all: ROACH {} vector accumulators synced successfully.".format(roach))
            except Exception as e:
                results[roach] = {"success":False, "error":e}
                self.logger.error("sync_start_all: ROACH {} syncing vector accumulators failed: {}".format(roach, e))
        self.sync_start_all.cb(results)

    @Pyro4.oneway
    @async.async_method
    def get_adc_samples_all(self):
        """Get ADC samples for all ROACHs"""
        results = {}
        for roach in xrange(1,5):
            try:
                samples = self.spec.get_adc_samples(roach)
                results[roach] = {"success":True, "samples":samples}
                self.logger.info("get_adc_samples_all: Got ADC samples for ROACH {}".format(roach))
            except Exception as e:
                results[roach] = {"success":False, "samples": None, "error":e}
                self.logger.error("get_adc_samples_all: Failed to get ADC samples for ROACH {}".format(roach))
        self.get_adc_samples_all.cb(results)

    @Pyro4.oneway
    @async.async_method
    def calibrate_adc(self, roach):
        """asynchronously run the ADC calibrate method"""
        try:
            self.spec.calibrate(roach)
            msg = "ROACH {} calibrated".format(roach)
            self.logger.info(msg)
        except Exception as e:
            msg = "ROACH {} calibration failed: {}".format(roach, e)
            self.logger.error(msg)
        self.calibrate_adc.cb({'status': msg})

    @Pyro4.oneway
    @async.async_method
    def initialize_adc(self, roach):
        """asynchronously run the ADC initialize method"""
        try:
            self.spec.initialize(roach)
            msg = "ROACH {} initialized".format(roach)
            self.logger.info(msg)
        except Exception as e:
            msg = "ROACH {} initialization failed: {}".format(roach, e)
            self.logger.error(msg)
        self.initialize_adc.cb({'status':msg})

    @Pyro4.oneway
    @async.async_method
    def set_fft_shift(self, roach):
        """asynchronously set fft shift to 63"""
        try:
            fft_shift_val = int(0b0000000000111111)
            self.spec.fft_shift_set(roach, fft_shift_val)
            msg = "FFT shift for ROACH {} set to {}".format(roach, fft_shift_val)
            self.logger.info(msg)
        except Exception as e:
            msg = "Setting FFT shift for ROACH {} failed: {}".format(roach, e)
            self.logger.error(msg)
        self.set_fft_shift.cb({'status': msg})

    @Pyro4.oneway
    @async.async_method
    def sync_start(self, roach):
        """asynchronously start vector accumulators"""
        try:
            self.spec.sync_start(roach)
            msg = "Vector accumulators synchronized for ROACH {}".format(roach)
            self.logger.info(msg)
        except Exception as e:
            msg = "Synchronizing vector accumulators for ROACH {} failed: {}".format(roach, e)
            self.logger.error(msg)
        self.sync_start.cb({'status': msg})


    @Pyro4.oneway
    @async.async_method
    def get_adc_samples(self, roach):
        """asynchronously get ADC samples"""
        try:
            samples = self.spec.get_adc_samples(roach)
            msg = "Got ADC samples for ROACH {}".format(roach)
            success = True
            self.logger.info(msg)
        except Exception as e:
            samples = None
            msg = "Getting ADC samples for ROACH {} failed.".format(roach)
            success = False
            self.logger.error(msg)
        self.get_adc_samples.cb({"status":msg, "samples":[roach, samples], "success":success})

    # ================================================Source Handling=================================================
    def calc_azel(self, name, src_dict):
        """
        Given a source dictionary and the source's name, calculate it's current AZ/EL and RA/DEC.
        Args:
            name (str): The name of the source
            src_dict (dict): The source dictionary from its respective JSON file.
        Returns:
            dict: src_dict with new az, el, ra and dec fields.
        """
        try:
            body = TAMS_Source(name=name,
                          ra=src_dict['ra'],
                          dec=src_dict['dec'],
                          flux=src_dict['flux'],
                          velo=src_dict['velocity'],
                          category=src_dict.get("category", None),
                          obs_data=src_dict.get('obs_data', None),
                          status=src_dict.get('status', None))
        except Exception as err:
            self.logger.error(err)

        body.compute(self._cdscc)
        body_dict = body.toDict()
        body_dict['az'] = np.rad2deg(body.az)
        body_dict['el'] = np.rad2deg(body.alt)
        body_dict['ra_current'] = np.rad2deg(body.ra)
        body_dict['dec_current'] = np.rad2deg(body.dec)


        return body_dict

    @Pyro4.oneway
    @async.async_method
    def get_sources(self, band=None, calc_azel=False, get_verifiers=True):
        """
        Get catalog/known maser source/observation information from sources.json file
        If band is not None, attempts to get sources in a specific band.
        If, for example, we wanted to specifiy the 21 GHz band, we would specify band=21.
        If band is None, then we return nothing. This marks a difference in the API
        Keyword Arguments:
            band (list): Only return sources in this band
            calc_azel (bool): Whether or not to calculate the current az/el for sources.
            get_verifiers (bool): Whether or not to send verifiers along with other source info.
        """
        ref_freq = 2.223508e1

        bands = {20: [19.98,21],
                 21: [20.98,22],
                 22: [21.98,23]}
        self.logger.debug("get_sources: Called. Band info: {}".format(band))
        def parse_velocity(v):
            """
            Parse velocity string from sources. Returns a velocity in km/s
            """
            if "=" in v:
                self.logger.error("Don't know how to parse this velocity specification yet.")
                return None
            elif v == "":
                return None
            else:
                v = float(v)
                if v < 1.0 and v > 0.0:
                    # this means we have a redshift
                    v_kms = v*c_kms
                else:
                    v_kms = v
                return v_kms

        src_filename = os.path.join(self.source_dir, "sources.json")
        with open(src_filename, "r") as src_file:
            srcs = json.load(src_file)

        response_srcs = {}
        if get_verifiers:
            response_srcs.update(self.get_verifiers(calc_azel=calc_azel))
        if calc_azel:
            self._cdscc.date = datetime.datetime.utcnow()

        if band:
            if not isinstance(band, list):
                band = [band]
            self.logger.debug("Requested bands: {}".format(band))
            for b in band:
                b = int(b)
                for src_name in srcs:
                    src = srcs[src_name]
                    v = parse_velocity(str(src['velocity']))
                    if not v:
                        continue
                    freq = self.calc_obs_freq(ref_freq, v)
                    if freq > bands[b][0] and freq < bands[b][1]:
                        response_srcs[src_name] = src
                        if calc_azel:
                            src = self.calc_azel(src_name, src)
                        src['velocity'] = v
                        src['obs_freq'] = freq
                        response_srcs[src_name] = src
        self.get_sources.cb(response_srcs)

    @Pyro4.oneway
    @async.async_method
    def get_verifiers(self, calc_azel=False):
        """
        Get verifier (calibrator) information from verifiers.json file
        """
        verifier_filename = os.path.join(self.source_dir, "verifiers.json")
        with open(verifier_filename, "r") as verifier_file:
            verifiers = json.load(verifier_file)
        for name in verifiers:
            verifiers[name]['category'] = 'calibrator'
            calib = verifiers[name]
            if calc_azel:
                verifiers[name] = self.calc_azel(name, calib)
        self.get_verifiers.cb(verifiers)
        return verifiers

    def calc_obs_freq(self, ref_freq, obs_velo):
        """
        From some reference frequency, specified in GHz, and some observed velocity,
        specified in km/s, return the observed frequency.
        Alternatively, one can specify the units in using astropy units.
        Args:
            ref_freq (float/astropy.units): Reference frequency in GHz
            obs_velo (float/astropy.units): Observed velocity, in km/s
        Returns:
            float: The observed frequency, in GHz
        """
        if (isinstance(ref_freq, astropy.units.Quantity) and
            isinstance(obs_velo, astropy.units.Quantity)):
            return (ref_freq / (1. + (obs_velo/c))).to("GHz").value
        else:
            return ref_freq / (1 + (obs_velo / c_kms))

    # ================================================Miscellenous====================================================
    @Pyro4.oneway
    @async.async_method
    def get_simulator_status(self):
        """Get the simulator status for all hardware controllers"""
        self.logger.debug("get_simulator_status: Called.")
        status = {}
        status['apc_wsn'] = self.apc.wsn
        status['apc'] = self.apc.simulated
        status['wbdc_fe'] = self.wbdc_fe.simulated
        status['spec'] = self.spec.simulated
        status['dss43'] = self.simulated
        self.logger.info("get_simulator_status: Status: {}".format(status))
        self.get_simulator_status.cb(status)

    @Pyro4.oneway
    @async.async_method
    def connect_to_hardware(self, controller, *args):
        if controller == 'apc':
            self.logger.debug("Connecting APC to hardware, workstation {}".format(args[0]))
        else:
            self.logger.debug("Connecting {} to hardware".format(controller))
        self.hdwr_method(controller, "connect_to_hardware", *args)
        self.connect_to_hardware.cb(controller)

    @Pyro4.oneway
    @async.async_method
    def connect_to_simulator(self, controller):
        self.logger.debug("Connecting {} to simulator".format(controller))
        self.hdwr_method(controller, "simulate")
        self.connect_to_simulator.cb(controller)

    def get_sidereal_time(self):
        """
        Get the current sidereal time from the _cdscc object.
        Returns:
            str: The formatted sidereal time string
        """
        self._cdscc.date = datetime.datetime.utcnow()
        return str(self._cdscc.sidereal_time())

    def get_weather(self):
        """
        Get the current weather in the area around the antenna.
        weather.get_current_weather returns a requests.Request object, from which we can get JSON data.
        Note that this is distinct from the APC "GET_WTHR" method, which will return locally recorded
        weather data.

        This uses the OpenWeatherMap API to get weather data.
        """
        weather_req = weather.get_current_weather(np.rad2deg(float(self._cdscc.lat)),
                                                  np.rad2deg(float(self._cdscc.lon)))
        if weather_req.status_code != 200:
            return {"success": False}
        else:
            weather_dict = weather_req.json()
            self.logger.debug("Weather dict: {}".format(weather_dict))
            humidity = weather_dict['main']['humidity']
            pressure = weather_dict['main']['pressure']
            temp = weather_dict['main']['temp']
            windspeed = weather_dict['wind']['speed']
            try:
                winddir = weather_dict['wind']['deg']
            except KeyError as err:
                # sometimes this will fail
                winddir = None

            description = weather_dict['weather'][0]['description']

            return {'success': True,
                    'pressure': pressure,
                    'humidity': humidity,
                    'temp': temp,
                    'windspeed': windspeed,
                    'winddir': winddir,
                    'description': description}


    def welchs_t_test(self, mean1, mean2, sigma1, sigma2, n1, n2):
        """
        Calculate Welch's t-test for two distributions, given their means, sigmas, and the number of samples.
        """
        t = (mean1 - mean2) / (np.sqrt((sigma1**2 / n1) + (sigma2**2 / n2)))
        mu = ((sigma1**2/n1) + (sigma2**2/n2))**2 / ((sigma1**4/(n1**2 * (n1-1))) + (sigma2**4/(n2**2 * (n2-1))))
        return t, mu


def main():

    from support.arguments import simple_parse_args
    parser = simple_parse_args("Launch (deprecated) DSS43K2 master server").parse_args()
    dss43 = DSS43K2Server(simulated=parser.simulated, ns_port=parser.ns_port)
    dss43.launch_server(threaded=False, ns=True, objectPort=50001, local=parser.local)

if __name__ == '__main__':
    main()
