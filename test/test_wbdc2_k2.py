import unittest

from MonitorControl.Configurations.CDSCC.WBDC2_K2 import station_configuration

class TestWBDCK2Configuration(unittest.TestCase):

    def test_dss43_station_configuration(self):
        observatory, equipment = station_configuration()

if __name__ == "__main__":
    unittest.main()
