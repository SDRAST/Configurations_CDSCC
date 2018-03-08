"""
Test boresight, and boresight related functions, such as the pm_integrator method, and
the calc_bs_points method.
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

from support.pyro import config
from tams_source import TAMS_Source
from MonitorControl.Configurations.CDSCC.apps.server.dss43k2_server import DSS43K2Server

from . import DSS43Client, wait_for_callback
from ... import setup_logging


class TestDSS43K2PointOnSource(unittest.TestCase):

    isSetup = False

    def setUp(self):
        if not self.__class__.isSetup:
            cb_names = ["point_onsource_cb", "point_onsource_cb_updates"]
            DSS43Client.define_callbacks(cb_names)
            port = 50000
            server_logger = logging.getLogger("TestDSS43Server")
            server_logger.setLevel(logging.DEBUG)
            client_logger = logging.getLogger("TestDSS43Client")
            server = DSS43K2Server(logger=server_logger)
            server_thread = server.launch_server(ns_port=port, local=True, threaded=True)
            DSS43Client.generate_callbacks()
            self.__class__.client = DSS43Client(server.name, port, logger=client_logger)
            self.__class__.isSetup = True
        else:
            pass

    def test_point_onsource(self):
        """
        Test the entire boresight routine
        """
        cb_name = "point_onsource_cb"
        cb_updates_name = "point_onsource_cb_updates"
        client = self.__class__.client

        test_src = TAMS_Source(
            name="0537-441",
            ra=1.478465645926414,
            dec=-0.7694426542639248
        )

        client.proxy.point_onsource(test_src.toDict(), cb_info={
            "cb_handler":client,
            "cb":cb_name,
            "cb_updates": cb_updates_name
        })

        def updates_check():
            """
            Check to see if the updates_cb is returning the correct information
            """
            data = wait_for_callback(client, cb_updates_name)
            self.assertTrue(isinstance(data, dict))

        result = wait_for_callback(client, cb_name, secondary_cb=updates_check)
        self.assertTrue(isinstance(result, dict))

if __name__ == "__main__":

    main_logger = logging.getLogger("TestDSS43K2PointOnSource")
    main_logger.setLevel(logging.DEBUG)

    suite_basic = unittest.TestSuite()
    suite_basic.addTest(TestDSS43K2PointOnSource("test_point_onsource"))

    # suite_advanced.addTest(TestDSS43K2PointOnSource("test_boresight"))
    result_basic = unittest.TextTestRunner().run(suite_basic)
    # if result_basic.wasSuccessful():
    #     unittest.TextTestRunner().run(suite_advanced)
