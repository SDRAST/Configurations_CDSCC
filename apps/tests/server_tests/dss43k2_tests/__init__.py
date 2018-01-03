import logging
import sys
import time
import threading

import Pyro4

import pyro4tunneling

from support.pyro import config

def wait_for_callback(client, cb_name, secondary_cb=None):
    with client.lock:
        cb_called = client.test_status[cb_name]['status']
    while not cb_called:
        time.sleep(0.1)
        with client.lock:
            cb_called = client.test_status[cb_name]['status']
        if secondary_cb:
            secondary_cb()
    with client.lock:
        data = client.test_status[cb_name]['data']
    return data

class DSS43Client(object):

    cb_names = []

    def __init__(self, server_name, port, logger=None):
        self.daemon = Pyro4.Daemon()
        self.daemon.register(self)
        self.daemon_thread = threading.Thread(target=self.daemon.requestLoop)
        self.daemon_thread.daemon = True
        self.daemon_thread.start()

        ns = Pyro4.locateNS(port=port)
        self.proxy = Pyro4.Proxy(ns.lookup(server_name))
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(module_logger.name + ".DSS43TestClient")

        self.lock = threading.Lock()

        self.test_status = {cb_name: {"status": False, "data": None} for cb_name in self.cb_names}

    @classmethod
    def define_callbacks(cls, callbacks):
        """
        Define the class attribute cb_names
        """
        if not isinstance(callbacks, list):
            callbacks = [callback]
        cls.cb_names = callbacks

    @classmethod
    def generate_callbacks(cls):
        """
        Generate @expose'd callback methods that will get called by tests.
        """
        def callback_factory(name):
            def callback(self, data=None):
                with self.lock:
                    self.test_status[name]['status'] = True
                    self.test_status[name]['data'] = data
                self.logger.debug("{}: Called.".format(name))
            return callback

        for cb_name in cls.cb_names:
            setattr(cls, cb_name, config.expose(callback_factory(cb_name)))
