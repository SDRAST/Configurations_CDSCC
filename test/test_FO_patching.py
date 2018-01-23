import json
import logging
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

    def test_get_patching(self):
        """
        test whether get_patching returns expected results
        """
        test_patching_json_path = os.path.join(test_dir, "test_patching.json")
        with open(test_patching_json_path, "r") as f:
            test_patching = json.load(f)

        test_file_name = "test_FO_patching.xlsx"
        da = DistributionAssembly(parampath=test_dir,paramfile=test_file_name)
        patching = da.get_patching()
        for key in patching:
            self.assertDictEqual(patching[key], test_patching[str(key)])



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
