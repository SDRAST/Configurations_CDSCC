import os
import unittest

from MonitorControl.Configurations.CDSCC.FO_patching import DistributionAssembly, modulepath, paramfile

test_dir = os.path.dirname(os.path.abspath(__file__)) + "/"

class TestFO_Patching(unittest.TestCase):

    def test_init_default_args(self):

        da = DistributionAssembly()
        self.assertTrue(da.parampath == modulepath)
        self.assertTrue(da.paramfile == paramfile)

    def test_init_custom_args(self):

        test_file_name = "test_FO_patching.xlsx"
        da = DistributionAssembly(parampath=test_dir,paramfile=test_file_name)
        self.assertTrue(da.parampath == test_dir)
        self.assertTrue(da.paramfile == test_file_name)

if __name__ == "__main__":
    unittest.main()
