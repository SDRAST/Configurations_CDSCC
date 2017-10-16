import unittest
import logging

from TAMS_BackEnd.automation import AutomaticTest
from TAMS_BackEnd.servers import WBDCFrontEndServer

class TestWBDCFrontEnd(unittest.TestCase):

    isSetup = False
    # server = WBDCFrontEndServer(simulated=True, loglevel=logging.DEBUG)

    def setUp(self):

        if not self.isSetup:
            self.__class__.isSetup = True
            self.__class__.server = WBDCFrontEndServer(simulated=False,
                                                       loglevel=logging.DEBUG,
                                                       logfile="./WBDCtest.log")
        else:
            pass

if __name__ == '__main__':

    runner = unittest.TextTestRunner()
    auto = AutomaticTest(TestWBDCFrontEnd, WBDCFrontEndServer)
    suite = auto.create_test_suite()
    result_basic = runner.run(suite)