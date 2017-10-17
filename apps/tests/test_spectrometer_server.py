import unittest
import logging

from TAMS_BackEnd.automation import AutomaticTest
from MonitorControl.BackEnds.ROACH1.apps.server.SAO_pyro4_server import SpectrometerServer


class TestSpectrometer(unittest.TestCase):

    isSetup = False

    # server = WBDCFrontEndServer(simulated=True, loglevel=logging.DEBUG)

    def setUp(self):

        if not self.isSetup:
            self.__class__.isSetup = True
            self.__class__.server = SpectrometerServer("Spec",
                                                       simulated=True,
                                                       loglevel=logging.DEBUG,
                                                       logfile="./WBDCtest.log")
        else:
            pass


if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    auto = AutomaticTest(TestSpectrometer, SpectrometerServer)
    suite = auto.create_test_suite()
    result_basic = runner.run(suite)