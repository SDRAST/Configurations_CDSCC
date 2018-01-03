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


class TestDSS43K2Boresight(unittest.TestCase):

    isSetup = False

    def setUp(self):
        if not self.__class__.isSetup:
            cb_names = ["boresight_cb", "boresight_cb_updates"]
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

    def test_pm_integrator(self):
        """
        Test pm_integrator method
        """
        client = self.__class__.client
        pm1_mean = client.proxy.pm_integrator()
        self.assertTrue(isinstance(pm1_mean, float))

    def test_pm_integrator_all(self):
        """
        Test pm_integrator_all method
        """
        client = self.__class__.client
        pm_mean = client.proxy.pm_integrator_all()
        self.assertTrue(len(pm_mean) == 4 and isinstance(pm_mean[0], float))

    def test_calc_bs_points(self):
        """
        Test whether we can get 9 boresight points from the calc_bs_points method
        """
        client = self.__class__.client
        logger = logging.getLogger("TestDSS43Client.test_calc_bs_points")
        for i in range(6, 15):
            points = client.proxy.calc_bs_points(n_points=i)
            logger.debug(points)
            self.assertTrue(len(points[0]) == i)

    def test_grab_pm_data(self):
        """
        Test the grab_pm_data method
        """
        client = self.__class__.client
        logger = logging.getLogger("TestDSS43Client.test_grab_pm_data")

        points = [-4.5, 0]
        self.client.proxy.set_boresight_running(True)
        integration = client.proxy.grab_pm_data("el", points)
        self.client.proxy.set_boresight_running(False)
        self.assertTrue(isinstance(integration, list))
        self.assertTrue(isinstance(integration[0], list))

    def test_boresight(self):
        """
        Test the entire boresight routine
        """
        cb_name = "boresight_cb"
        cb_updates_name = "boresight_cb_updates"

        client = self.__class__.client
        client.proxy.boresight(0.0, 0.0, n_points=6, cb_info={
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

        # sanity check to see if we have all the keys we're looking for
        self.assertTrue("prog" in result)
        self.assertTrue("fit_results" in result)
        self.assertTrue("iter" in result)
        self.assertTrue("total_iter" in result)
        self.assertTrue("delta_offsets" in result)
        self.assertTrue("fields" in result)
        fields = result['fields']

        # type check boresight results
        self.assertTrue(isinstance(result['prog'], list))
        self.assertTrue(isinstance(result['fit_results'], dict))
        self.assertTrue(isinstance(result['iter'], int))
        self.assertTrue(isinstance(result['total_iter'], int))
        self.assertTrue(isinstance(result['delta_offsets'], dict))
        self.assertTrue(isinstance(result['fields'], list))

        # now make sure boresight results have correct data
        self.assertTrue(all(f in result['delta_offsets']['el'] for f in fields))
        self.assertTrue(all(f in result['delta_offsets']['xel'] for f in fields))
        self.assertTrue(all(f in result['fit_results']['el'] for f in fields))
        self.assertTrue(all(f in result['fit_results']['xel'] for f in fields))

if __name__ == "__main__":

    main_logger = logging.getLogger("TestDSS43K2Boresight")
    main_logger.setLevel(logging.DEBUG)

    suite_basic = unittest.TestSuite()
    suite_advanced = unittest.TestSuite()
    suite_basic.addTest(TestDSS43K2Boresight("test_pm_integrator"))
    suite_basic.addTest(TestDSS43K2Boresight("test_pm_integrator_all"))
    suite_basic.addTest(TestDSS43K2Boresight("test_grab_pm_data"))
    suite_basic.addTest(TestDSS43K2Boresight("test_calc_bs_points"))

    suite_advanced.addTest(TestDSS43K2Boresight("test_boresight"))
    result_basic = unittest.TextTestRunner().run(suite_basic)
    if result_basic.wasSuccessful():
        unittest.TextTestRunner().run(suite_advanced)
