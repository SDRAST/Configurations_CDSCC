"""
Test basic functionality of DSS43K2Client
"""
import unittest
import logging
import sys
import time
import threading

import Pyro4
import Pyro4.naming
import Pyro4.socketutil

import pyro4tunneling

from MonitorControl.Configurations.CDSCC.apps.server.dss43k2_server import DSS43K2Server
from MonitorControl.Configurations.CDSCC.apps.client.dss43k2_client import DSS43K2Client

from .. import setup_logging


class TestDSS43K2Client(unittest.TestCase):

    isSetup = False

    def setUp(self):
        if not self.__class__.isSetup:
            host, port = "localhost", 9090
            server_logger = logging.getLogger("TestDSS43Server")
            server_logger.setLevel(logging.DEBUG)
            client_logger = logging.getLogger("TestDSS43Client")
            server = DSS43K2Server(ns_port=port, ns_host=host, logger=server_logger, simulated=True)
            server_thread = server.launch_server(ns_host=host, ns_port=port, local=True, threaded=True)
            tunnel = pyro4tunneling.Pyro4Tunnel(ns_host=host, ns_port=port, local=True)
            self.__class__.client = DSS43Client(tunnel, server.name, port=port, logger=client_logger)
            self.__class__.isSetup = True
        else:
            pass

    def test_get_azel(self):
        """
        Test pm_integrator method
        """
        client = self.__class__.client
        azel = client.get_azel()


if __name__ == "__main__":

    main_logger = logging.getLogger("TestDSS43K2Client")
    main_logger.setLevel(logging.DEBUG)

    suite_basic = unittest.TestSuite()
    # suite_advanced = unittest.TestSuite()
    suite_basic.addTest(TestDSS43K2Client("test_get_azel"))
    # suite_basic.addTest(TestDSS43K2Boresight("test_pm_integrator_all"))
    # suite_basic.addTest(TestDSS43K2Boresight("test_grab_pm_data"))
    # suite_basic.addTest(TestDSS43K2Boresight("test_calc_bs_points"))

    # suite_advanced.addTest(TestDSS43K2Boresight("test_boresight"))
    result_basic = unittest.TextTestRunner().run(suite_basic)
    # if result_basic.wasSuccessful():
    #     unittest.TextTestRunner().run(suite_advanced)
