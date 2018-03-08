import unittest

from TAMS_BackEnd.automation import AutomaticTest
from TAMS_BackEnd.examples.basic_pyro4_server import BasicServer

class TestExample(unittest.TestCase):

    isSetup = False

    def setUp(self):

        if not self.isSetup:
            self.__class__.isSetup = True
            self.__class__.server = BasicServer()
        else:
            pass

if __name__ == '__main__':

    runner = unittest.TextTestRunner()
    auto = AutomaticTest(BasicServer)
    suite = auto.create_test_suite(TestExample)
    result_basic = runner.run(suite)