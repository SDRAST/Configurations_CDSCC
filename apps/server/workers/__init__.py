from __future__ import print_function
import logging
import datetime
import time

import Pyro4

from pyro_support import async_method
from support.threading_util import PausableThread, iterativeRun

module_logger = logging.getLogger(__name__)

class LongRunningWorker(PausableThread):

    @async_method
    def __init__(self, parent, client, method, method_args=None, method_kwargs=None,
                        logger=None, name="LongRunningWorker", **kwargs):
        PausableThread.__init__(self, name=name, **kwargs)
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(module_logger.name + "." + self.name)
        self.parent = parent
        self.client = client
        self.method = method

        if not method_args:
            self.method_args = ()
        else:
            self.method_args = method_args

        if not method_kwargs:
            self.method_kwargs = {}
        else:
            self.method_kwargs = method_kwargs

    def run(self):

        try:
            result = getattr(self.client, self.method)(*self.method_args, **self.method_kwargs)
            self.cb(result)
        except Exception as err:
            self.logger.error("Method {} failed with error {}".format(self.method, err))
            self.cb(None)

class RMSWorker(PausableThread):
    """
    A thread that asks for Power Meter information at a set interval
    """
    @async_method
    def __init__(self, parent, client, update_rate, logger=None, **kwargs):
        PausableThread.__init__(self, name='RMSWorker', **kwargs)
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(module_logger.name + ".RMSWorker")
        self.parent = parent
        self.spec_client = client
        self.update_rate = update_rate
        self.connection_closed_errors = 0
        self.communication_errors = 0
        self.max_connection_closed_errors = 3
        self.max_communication_errors = 3
        self.logger.debug("self.cb: {}".format(self.cb))
        self.logger.debug("self.cb_updates: {}".format(self.cb_updates))

    @iterativeRun
    def run(self):
        timestamp = datetime.datetime.utcnow()
        rms_info = {}
        for i in xrange(1,5):
            rms_info[i] = self.spec_client.calc_rms(i)
        rms_info['timestamp'] = timestamp.strftime("%j-%Hh%Mm%Ss")
        try:
            self.cb_updates(rms_info)
        except Pyro4.errors.ConnectionClosedError as err:
            self.logger.error("ConnectionClosedError: {}".format(err))
            self.connection_closed_errors += 1
        except Pyro4.errors.CommunicationError as err:
            self.logger.error("CommunicationError: {}".format(err))
            self.communication_errors += 1
        if self.connection_closed_errors >= self.max_connection_closed_errors:
            self.cb_updates = lambda *args, **kwargs: None
        if self.communication_errors >= self.max_communication_errors:
            self.cb_updates = lambda *args, **kwargs: None
        time.sleep(self.update_rate)

    def set_callback(self, cb_info):
        pass

    def set_update_time(self, update_rate):
        """
        Change the rate at which the worker updates
        Args:
            update_rate (float/int): The new update rate
        """
        with self._lock:
            self.update_rate = update_rate


class PowerMeterWorker(PausableThread):
    """
    A thread that asks for Power Meter information at a set interval
    """
    @async_method
    def __init__(self, parent, wbdc_client, update_rate,logger=None, **kwargs):
        PausableThread.__init__(self, name='PowerMeterWorker', **kwargs)
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(module_logger.name + ".PMWorker")
        self.parent = parent
        self.wbdc_client = wbdc_client
        self.update_rate = update_rate
        self.connection_closed_errors = 0
        self.communication_errors = 0
        self.max_connection_closed_errors = 3
        self.max_communication_errors = 3
        self.logger.debug("self.cb: {}".format(self.cb))
        self.logger.debug("self.cb_updates: {}".format(self.cb_updates))

    @iterativeRun
    def run(self):
        tsys = self.wbdc_client.get_tsys()
        timestamp = datetime.datetime.utcnow()
        self.logger.debug("Tsys info: {}".format(tsys['tsys']))
        with self.parent.lock:
            tsys_info = {'timestamp':timestamp.isoformat(),
                         'tsys': tsys['tsys'],
                         'pm_readings': tsys['pm_readings']}
            self.parent.tsys_info = tsys_info
            try:
                self.cb_updates(tsys_info)
            except Pyro4.errors.ConnectionClosedError as err:
                self.logger.error("ConnectionClosedError: {}".format(err))
                self.connection_closed_errors += 1
            except Pyro4.errors.CommunicationError as err:
                self.logger.error("CommunicationError: {}".format(err))
                self.communication_errors += 1

        if self.connection_closed_errors >= self.max_connection_closed_errors:
            self.cb_updates = lambda *args, **kwargs: None
        if self.communication_errors >= self.max_communication_errors:
            self.cb_updates = lambda *args, **kwargs: None

        time.sleep(self.update_rate)

    def set_callback(self, cb_info):
        pass

    def set_update_time(self, update_rate):
        """
        Change the rate at which the worker updates
        Args:
            update_rate (float/int): The new update rate
        """
        with self._lock:
            self.update_rate = update_rate

class APCWorker(PausableThread):
    """
    A thread that asks for APC information
    (offsets, current az/el, whether antenna is on source)
    at a set interval
    """
    @async_method
    def __init__(self, parent, apc_client, update_rate, logger=None, **kwargs):

        PausableThread.__init__(self, name='APCWorker', **kwargs)
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(module_logger.name + ".APCWorker")
        self.parent = parent
        self.apc_client = apc_client
        self.update_rate = update_rate
        self.connection_closed_errors = 0
        self.communication_errors = 0
        self.max_connection_closed_errors = 3
        self.max_communication_errors = 3
        self.logger.debug("self.cb: {}".format(self.cb))
        self.logger.debug("self.cb_updates: {}".format(self.cb_updates))

    @iterativeRun
    def run(self):

        offsets = self.apc_client.get_offsets()
        azel = self.apc_client.get_azel()
        onsource = self.apc_client.onsource()
        timestamp = datetime.datetime.utcnow()
        self.logger.debug("onsource info: {}, azel info: {}, offsets: {}".format(onsource, azel, offsets))
        with self.parent.lock:
            apc_info = {'timestamp':timestamp.isoformat(),
                                    'offsets':offsets,
                                    'azel':azel,
                                    'onsource':onsource}
            self.parent.apc_info = apc_info
            try:
                self.cb_updates(apc_info)
            except Pyro4.errors.ConnectionClosedError as err:
                self.logger.error("ConnectionClosedError: {}".format(err))
                self.connection_closed_errors += 1
            except Pyro4.errors.CommunicationError as err:
                self.logger.error("CommunicationError: {}".format(err))
                self.communication_errors += 1
        if self.connection_closed_errors >= self.max_connection_closed_errors:
            self.cb_updates = lambda *args, **kwargs: None
        if self.communication_errors >= self.max_communication_errors:
            self.cb_updates = lambda *args, **kwargs: None
        time.sleep(self.update_rate)

    def set_callback(self, cb_info):
        pass

    def set_update_time(self, update_rate):
        """
        Change the rate at which the worker updates
        Args:
            update_rate (float/int): The new update rate
        """
        with self._lock:
            self.update_rate = update_rate

class TwoBeamNodWorker(PausableThread):

    @async_method
    def __init__(self, parent, apc_client, n_cycles, time_per_scan, src_obj, init_el, init_xel, **kwargs):
        """
        Run the nodding routine
        """
        PausableThread.__init__(self, name='TwoBeamNodThread', **kwargs)
        self.apc_client = apc_client
        self.n_cycles = n_cycles
        self.time_per_scan = time_per_scan
        self.src_obj = src_obj
        self.n_scan = -1 # the scan number
        self.completed_cycles = 0
        self.parent = parent
        self.eloffset = init_el
        self.xeloffset = init_xel

    def run(self):

        self._running.set()
        # offsets = self.apc_client.get_offsets()
        # eloffset, xeloffset = offsets['el'], offsets['xel']
        eloffset, xeloffset = self.eloffset, self.xeloffset

        self.logger.info("Initial offsets: {}, {}".format(eloffset, xeloffset))

        for n in xrange(self.n_cycles):
            self.logger.debug("run: running cycle {}".format(n))
            if self.stopped():
                self._running.clear()
                return
            self.n_scan = -1
            response = self.apc_client.point_onsource(self.src_obj.name, str(self.src_obj.ra), str(self.src_obj.dec))
            if response:
                for i in xrange(2):
                    if self.stopped():
                        self._running.clear()
                        return
                    self.n_scan = -1
                    self.cb_updates(
                        {'status': 'Observing, changing feed', 'feed_status': 0, 'scan': self.n_scan})
                    self.apc_client.feed_change(i, eloffset, xeloffset)
                    self.n_scan = 2*self.completed_cycles + i + 1
                    self.cb_updates(
                        {'status': 'Observing, on feed {}'.format(i+1), 'feed_status': i+1,'scan': self.n_scan})
                    t_init = time.time()
                    while time.time() - t_init < self.time_per_scan:
                        time.sleep(0.01)
                        if self.stopped():
                            self._running.clear()
                            return
                self.completed_cycles += 1

                # self.n_scan = -1
                # self.handler.obs_update_callback({'status': 'Observing, changing feed', 'feed_status': 0, 'scan': self.n_scan})
                # self.apc_client.feed_change(0, eloffset, xeloffset)
                # self.n_scan = 2*self.completed_cycles + 1
                # self.handler.obs_update_callback({'status': 'Observing, on feed 1', 'feed_status': 1, 'scan': self.n_scan})
                # time.sleep(self.time_per_scan)
                # if self.stopped():
                #     self._running.clear()
                #     return
                # self.n_scan = -1
                # self.handler.obs_update_callback({'status': 'Observing, changing feed', 'feed_status': 0, 'scan': self.n_scan})
                # self.apc_client.feed_change(1, eloffset, xeloffset)
                # self.n_scan = 2*self.completed_cycles + 2
                # self.handler.obs_update_callback({'status': 'Observing, on feed 2', 'feed_status': 2, 'scan': self.n_scan})
                # time.sleep(self.time_per_scan)
                # self.completed_cycles += 1
                # if self.stopped():
                #     self._running.clear()
                #     return
            elif not response:
                self.n_scan = -1

            with self.parent.lock:
                self.parent.save_fits()

        self.apc_client.feed_change(0, eloffset, xeloffset)
        self._running.clear()
        with self.parent.lock:
            self.logger.debug("Calling stop parent.stop_nodding")
            self.parent.stop_nodding()

    def get_scan(self):
        """
        NEED TO USE A LOCK TO ACCESS THIS.
        """
        return self.n_scan
