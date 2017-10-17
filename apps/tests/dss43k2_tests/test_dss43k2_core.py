"""
Test core functionality of the dss43k2 server/client. This includes things
like asynchronously getting antenna Az/El, power meter readings, and setting ADC gain.
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

from tams_source import TAMS_Source
from pyro_support import config
from DSS43Backend.servers.dss43k2_server import DSS43K2Server

from . import DSS43Client, wait_for_callback
from .. import setup_logging

class TestDSS43K2Core(unittest.TestCase):

    isSetup = False

    def setUp(self):
        if not self.__class__.isSetup:
            cb_names = ["get_sources_cb", "get_azel_cb",
                        "get_offsets_cb", "set_offset_el_cb",
                        "set_offset_xel_cb", "onsource_cb",
                        "calc_rms_cb", "set_adc_gain_cb",
                        "calibrate_adc_all_cb", "initialize_adc_all_cb",
                        "set_fft_shift_all_cb", "sync_start_all_cb"]

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


    #================== Catalog tests ==================
    def test_get_sources(self):
        """
        Test whether we can get sources from the proxy using callbacks
        """
        cb_name = "get_sources_cb"
        client = self.__class__.client
        client.proxy.get_sources(band=[21], cb_info={"cb_handler":client,
                                                          "cb": cb_name})
        self.assertIsNotNone(wait_for_callback(client, cb_name))

    # =================== APC tests ====================

    def test_get_azel(self):
        """
        Test whether we can asynchronously get Az/El information
        """
        cb_name = "get_azel_cb"
        client = self.__class__.client
        client.proxy.get_azel(cb_info={"cb_handler":client,
                                                          "cb": cb_name})
        self.assertIsNotNone(wait_for_callback(client, cb_name))

    def test_get_offsets(self):
        """
        Test whether we can asynchronously get antenna offsets.
        """
        cb_name = "get_offsets_cb"
        client = self.__class__.client
        client.proxy.get_offsets(cb_info={"cb_handler":client,
                                                          "cb": cb_name})
        self.assertIsNotNone(wait_for_callback(client, cb_name))

    def test_onsource(self):
        """
        Test whether we can asynchronously get antenna status.
        """
        cb_name = "onsource_cb"
        client = self.__class__.client
        client.proxy.onsource(cb_info={"cb_handler":client,
                                                          "cb": cb_name})
        self.assertIsNotNone(wait_for_callback(client, cb_name))

    def test_set_offset_el(self):
        """
        Test whether we can asynchronously set El offset.
        """
        cb_name = "set_offset_el_cb"
        client = self.__class__.client
        client.proxy.set_offset_el(0.0, cb_info={"cb_handler":client,
                                                          "cb": cb_name})
        self.assertIsNotNone(wait_for_callback(client, cb_name))

    def test_set_offset_xel(self):
        """
        Test whether we can asynchronously set Xel offset.
        """
        cb_name = "set_offset_xel_cb"
        client = self.__class__.client
        client.proxy.set_offset_xel(0.0, cb_info={"cb_handler":client,
                                                          "cb": cb_name})
        self.assertIsNotNone(wait_for_callback(client, cb_name))

    # =================== Spectrometer Tests ======================
    def test_calc_rms(self):
        """
        Test whether we can asynchronously calculate RMS for given ROACH.
        """
        cb_name = "calc_rms_cb"
        client = self.__class__.client
        client.proxy.calc_rms(1, cb_info={"cb_handler":client,
                                                          "cb": cb_name})
        self.assertIsNotNone(wait_for_callback(client, cb_name))

    def test_set_adc_gain(self):
        """
        Test whether we can asynchronously set ADC gain for ROACH 1 to 1.0.
        """
        cb_name = "set_adc_gain_cb"
        client = self.__class__.client
        client.proxy.set_adc_gain(1, 10.0, cb_info={"cb_handler":client,
                                                          "cb": cb_name})
        self.assertIsNotNone(wait_for_callback(client, cb_name))

    def test_calibrate_adc_all(self):
        """
        Test whether we can asynchronously calibrate all ADCs
        """
        cb_name = "calibrate_adc_all_cb"
        client = self.__class__.client
        client.proxy.calibrate_adc_all(cb_info={"cb_handler":client,
                                                          "cb": cb_name})
        self.assertIsNotNone(wait_for_callback(client, cb_name))

    def test_initialize_adc_all(self):
        """
        Test whether we can asynchronously initialize all ADCs
        """
        cb_name = "initialize_adc_all_cb"
        client = self.__class__.client
        client.proxy.initialize_adc_all(cb_info={"cb_handler":client,
                                                          "cb": cb_name})
        self.assertIsNotNone(wait_for_callback(client, cb_name))

    def test_set_fft_shift_all(self):
        """
        Test whether we can asynchronously initialize all ADCs
        """
        cb_name = "set_fft_shift_all_cb"
        client = self.__class__.client
        client.proxy.set_fft_shift_all(cb_info={"cb_handler":client,
                                                          "cb": cb_name})
        self.assertIsNotNone(wait_for_callback(client, cb_name))

    def test_sync_start_all(self):
        """
        Test whether we can asynchronously initialize all ADCs
        """
        cb_name = "sync_start_all_cb"
        client = self.__class__.client
        client.proxy.sync_start_all(cb_info={"cb_handler":client,
                                                          "cb": cb_name})
        self.assertIsNotNone(wait_for_callback(client, cb_name))


if __name__ == '__main__':
    main_logger = logging.getLogger("TestDSS43K2Core")
    main_logger.setLevel(logging.DEBUG)

    suite_basic = unittest.TestSuite()
    suite_advanced = unittest.TestSuite()

    suite_basic.addTest(TestDSS43K2Core("test_get_sources"))

    suite_basic.addTest(TestDSS43K2Core("test_get_azel"))
    suite_basic.addTest(TestDSS43K2Core("test_get_offsets"))
    suite_basic.addTest(TestDSS43K2Core("test_onsource"))
    suite_basic.addTest(TestDSS43K2Core("test_set_offset_el"))
    suite_basic.addTest(TestDSS43K2Core("test_set_offset_xel"))

    # suite_basic.addTest(TestDSS43K2Core("test_calc_rms"))
    # suite_basic.addTest(TestDSS43K2Core("test_set_adc_gain"))
    # suite_basic.addTest(TestDSS43K2Core("test_calibrate_adc_all"))
    # suite_basic.addTest(TestDSS43K2Core("test_initialize_adc_all"))
    # suite_basic.addTest(TestDSS43K2Core("test_set_fft_shift_all"))
    # suite_basic.addTest(TestDSS43K2Core("test_sync_start_all"))

    result_basic = unittest.TextTestRunner().run(suite_basic)
    if result_basic.wasSuccessful():
        unittest.TextTestRunner().run(suite_advanced)
