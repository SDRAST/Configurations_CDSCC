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

class TestDSS43K2Nodding(unittest.TestCase):

    isSetup = False

    def setUp(self):
        if not self.__class__.isSetup:
            cb_names = ["nodding_cb", "nodding_cb_updates"]
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

    def test_nodding(self):

        nodding_cb_updates_name = "nodding_cb_updates"
        nodding_cb_name = "nodding_cb"

        test_src_dict = {
            "status": None,
            "category": "known maser",
            "flux": 0,
            "ra": 0.29250263822381634,
            "velocity": "5047",
            "dec": -1.4016351371821572,
            "obs_data": [
                {
                    "link": "http://www.gb.nrao.edu/~jbraatz/masergifs/eso013-g012.gif"
                }
            ]
        }

        test_src =  TAMS_Source(name="ESO 013-G012",
                      ra=test_src_dict['ra'],
                      dec=test_src_dict['dec'],
                      flux=test_src_dict['flux'],
                      velo=test_src_dict['velocity'],
                      category=test_src_dict["category"],
                      obs_data=test_src_dict.get('obs_data', None),
                      status=test_src_dict.get('status', None))

        client = self.__class__.client
        client.proxy.record_data(n_cycles=1, time_per_scan=10,
            src_obj=test_src.toDict(),cb_info={
            'cb_handler':client,
            'cb':nodding_cb_name,
            'cb_updates':nodding_cb_updates_name
        })
        self.assertIsNone(wait_for_callback(client, nodding_cb_name))

if __name__ == "__main__":
    main_logger = logging.getLogger("TestDSS43K2Nodding")
    main_logger.setLevel(logging.DEBUG)
    suite = unittest.TestSuite()
    suite.addTest(TestDSS43K2Nodding("test_nodding"))
    unittest.TextTestRunner().run(suite)
