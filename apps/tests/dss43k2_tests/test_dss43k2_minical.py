import unittest
import logging
import sys
import time
import threading

import Pyro4
import Pyro4.naming
import Pyro4.socketutil

Pyro4.config.COMMTIMEOUT = 0.0

import pyro4tunneling

from tams_source import TAMS_Source
from pyro_support import config
from DSS43Backend.servers.dss43k2_server import DSS43K2Server
import MonitorControl.FrontEnds.minical.process_minical as process_minical

from . import DSS43Client, wait_for_callback
from .. import setup_logging

class TestDSS43K2Minical(unittest.TestCase):

    isSetup = False

    def setUp(self):
        if not self.__class__.isSetup:
            cb_names = ["minical_cb",
                        "minical_new_cb", "minical_new_cb_updates"]
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

    def test_minical(self):
        cb_name = "minical_cb"
        client = self.__class__.client
        client.proxy.minical(cb_info={
            "cb_handler":client,
            "cb":cb_name,
        })
        result = wait_for_callback(client, cb_name)
        self.assertIsNotNone(result)

    def test_process_minical_calib(self):
        """
        This function not only calls the process_minical_calib DSS43K2Server method,
        but it also attempts to compare results of this method to results of equivalent
        FrontEnds.minical.process_minical method.
        """
        client = self.__class__.client
        logger = logging.getLogger("TestDSS43K2Minical.test_process_minical_calib")
        load = {u'load1': 303.42, u'12K': 13.75, u'load2': 297.6, u'70K': 68.56}
        calib = {'load': [3.01400000e-07, 3.01400000e-07, 2.10200000e-07, 5.51740000e-07],
                 'sky': [1.23160000e-07, 1.23160000e-07, 9.93640000e-08, 2.58660000e-07],
                 'load+ND': [3.34800000e-07, 3.34800000e-07, 2.50000000e-07, 6.70440000e-07],
                 'zero': [-6.00000000e-11, -6.00000000e-11, 3.23100000e-09, 1.93600000e-11],
                 'mode': 'W',
                 'sky+ND': [1.23120000e-07, 1.23120000e-07, 9.93420000e-08, 2.58520000e-07],
                 'Tload': [load['load1'],load['load1'],load['load2'],load['load2']]}

        result = client.proxy.process_minical_calib(calib)
        result1 = []
        for i in xrange(4):
            calib_i = {key: calib[key][i] for key in calib if key != "mode"}
            calib_i['mode'] = "W"
            result1.append(process_minical(calib_i))

        logger.debug("result: {}".format(result))
        logger.debug("result1: {}".format(result1))

        for i in xrange(4):
            gains = [j[i] for j in result['gains']]
            gains1 = result1[i][0]
            logger.debug("gains: {}".format(gains))
            logger.debug("gains1: {}".format(gains1))
            self.assertTrue(all(i == j for i,j in zip(gains, gains1)))

            linear = [j[i] for j in result['linear']]
            linear1 = result1[i][1]
            logger.debug("linear: {}".format(linear))
            logger.debug("linear1: {}".format(linear1))
            self.assertTrue(all(i == j for i,j in zip(linear, linear1)))

            quadratic = [j[i] for j in result['quadratic']]
            quadratic1 = result1[i][2]
            logger.debug("quadratic: {}".format(quadratic))
            logger.debug("quadratic1: {}".format(quadratic1))
            self.assertTrue(all(i == j for i,j in zip(quadratic, quadratic1)))

            Tnd = result['nd-temp'][i]
            Tnd1 = result1[i][3]
            logger.debug("Tnd: {}".format(Tnd))
            logger.debug("Tnd1: {}".format(Tnd1))
            self.assertTrue(Tnd == Tnd1)

            nonlin = result['non-linearity'][i]
            nonlin1 = result1[i][4]
            logger.debug("nonlin: {}".format(nonlin))
            logger.debug("nonlin1: {}".format(nonlin1))
            self.assertTrue(nonlin == nonlin1)


    def test_minical_new(self):
        cb_name = "minical_new_cb"
        cb_updates_name = "minical_new_cb_updates"
        client = self.__class__.client
        client.proxy.minical_new(cb_info={
            "cb_handler": client,
            "cb": cb_name,
            "cb_updates": cb_updates_name
        })

        def updates_check():
            """
            Check to see if the updates_cb is returning the correct information
            """
            data = wait_for_callback(client, cb_updates_name)
            self.assertTrue(isinstance(data, dict))

        result = wait_for_callback(client, cb_name, secondary_cb=updates_check)
        self.assertIsNotNone(result)

if __name__ == "__main__":

    main_logger = logging.getLogger("TestDSS43K2Minical")
    main_logger.setLevel(logging.DEBUG)

    suite_basic = unittest.TestSuite()
    suite_advanced = unittest.TestSuite()

    suite_basic.addTest(TestDSS43K2Minical("test_process_minical_calib"))
    suite_advanced.addTest(TestDSS43K2Minical("test_minical_new"))
    unittest.TextTestRunner().run(suite_basic)

    result_basic = unittest.TextTestRunner().run(suite_basic)
    # if result_basic.wasSuccessful():
    #     unittest.TextTestRunner().run(suite_advanced)
