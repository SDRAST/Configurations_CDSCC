"""
gbtidlfits.py

A class for creating FITS files that are compatible with GBTIDL

Uses a JSON file configuration file to define the structure of the SDFITS file.

"""
import os
import json
import re
import logging
import time
import datetime

import numpy as np

try:
    import pyfits
    BACKEND='pyfits'
except ImportError:
    import astropy.io.fits as pyfits
    BACKEND='astropy'
from support.logs import logging_config

cur_dir = os.path.dirname(os.path.realpath(__file__))
default_config_file = os.path.join(cur_dir, "gbtidlFitsConfig.json")

module_logger = logging.getLogger(__name__)

class GBTIDLFITSFile(object):

    fits_dtype =  {
        'L' : 'bool_',
        'X' : 'bool_',
        'B' : 'ubyte',
        'I' : 'int16',
        'J' : 'int32',
        'K' : 'int64',
        'A' : 'str',
        'E' : 'float32',
        'D' : 'float64',
        'C' : 'complex64',
        'M' : 'complex128',
        'P' : 'float32'
    }

    def __init__(self, filename, dataset_size=None, config_file=default_config_file, **kwargs):
        """

        Args:
            filename:
            config_file:
        """
        self.logger = logging.getLogger(module_logger.name + ".GBTIDLFITSFile")
        self.filename = filename
        with open(config_file, 'r') as f_config:
            self.config = json.load(f_config)

        self.hdus = pyfits.HDUList([])
        self.create_primary_hdu(self.config)

        if dataset_size:
            self.create_data_hdu(dataset_size, self.config)

    def __getitem__(self, item):
        """
        Access the the hdu units
        """
        return self.hdus[item]

    def __getattr__(self, item):
        """
        Access the HDULIST.
        """
        return getattr(self.hdus, item)

    @property
    def primaryHDU(self):
        """
        Get the primary HDU, if it exists.
        Returns:
            pyfits.primaryHDU
        """
        if len(self.hdus) > 0:
            for hdu in self.hdus:
                if isinstance(hdu, pyfits.PrimaryHDU):
                    return hdu
        else:
            return None

    def create_primary_hdu(self, config=None):
        """
        Create the primary hdu. Checks to see if one exists before proceeding.
        Args:
            config (dict): The configuration from a configuration file, containing the header information
        Returns:
            pyfits.PrimaryHDU populated with header data from config
        """
        if not config: config = self.config
        t0 = time.time()
        for hdu in self.hdus:
            if isinstance(hdu, pyfits.PrimaryHDU):
                self.logger.debug("There is already a primary HDU!")
                return

        hdu = pyfits.PrimaryHDU()
        existing_keys = hdu.header.keys()
        for header_line in config['header_primary']:
            card = pyfits.Card().fromstring(header_line)
            card.verify()
            if card.keyword in existing_keys:
                continue
                # del hdu.header[card.keyword]
            hdu.header.append(card)

        date_str = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        hdu.header['DATE'] = date_str
        # self.logger.debug(counter)
        # keys = hdu.header.keys()
        # keys.sort()
        # self.logger.debug(len(keys))
        # self.logger.debug(keys)

        self.hdus.append(hdu)
        self.logger.debug("Took {:.4f} seconds to generated primary HDU".format(time.time() - t0))
        return hdu

    def create_data_hdu(self, dataset_size, config=None):
        """
        Generate a data HDU. This method can be called more that once, and it will just append
        HDUs to the hdus attribute.
        Args:
            dataset_size (int): The size of the dataset to create.
        Keyword Args:
            config (dict): The configuration
        Returns:
            pyfits.BinTableHDU configuration specified by config argument
        """
        if not config: config = self.config
        t0 = time.time()
        cols = []

        re_pattern = "(\d+)([A-Z])" # match all numbers and uppercase letters
        counter = 0
        for name in config['columns']:

            column_entry = config['columns'][name]

            format = column_entry.get('format', None)
            format_match = re.search(re_pattern, format)
            dtype = self.fits_dtype[format_match.group(2)]
            unit = column_entry.get('unit', None)
            # self.logger.debug("dtype and unit for {}: {} {}".format(name, dtype, unit))
            dim = column_entry.get('dim', None)

            if dim:
                dim_np = dim.strip(")").strip("(")
                dim_np = dim_np.split(",")
                dim_np = [int(d) for d in dim_np]
                # self.logger.debug("dim for {}: {}".format(name, dim_np))
        #     print("name: {}, format: {}, unit: {}, dim: {}".format(name, format, unit, dim))
            if BACKEND == 'pyfits' and pyfits.__version__ >= 3.4 and dim:
                final_shape = [dataset_size] + dim_np[::-1]
            elif BACKEND == 'astropy' and dim:
                final_shape = [dataset_size] + dim_np[::-1]
            else:
                final_shape = dataset_size
            array = np.zeros(final_shape) #, dtype=dtype)
            # print("{}, Final shape: {}, dtype: {}, array: {}".format(name, final_shape, dtype, array))
            cols.append(pyfits.Column(name=name, format=format, unit=unit, dim=dim, array=array))
            counter += 1

        table_data_hdu = pyfits.BinTableHDU.from_columns(pyfits.ColDefs(cols))
        existing_keys = table_data_hdu.header.keys()
        for data_line in config['header_data']:

            card = pyfits.Card().fromstring(data_line)
            card.verify()
            if card.keyword in existing_keys:
                continue
                # del table_data_hdu.header[card.keyword]
            table_data_hdu.header.append(card)

        self.hdus.append(table_data_hdu)
        self.logger.debug("Took {:.4f} seconds to generated data HDU".format(time.time() - t0))
        return table_data_hdu

    def write_to_file(self, overwrite=True):
        """
        Write the HDUs to file
        Returns:
            None
        """
        t0 = time.time()
        self.hdus.verify()
        if BACKEND == 'astropy':
            self.hdus.writeto(self.filename, overwrite=overwrite)
        elif BACKEND == 'pyfits':
            self.hdus.writeto(self.filename, clobber=overwrite)
        self.logger.debug("Took {:.4f} seconds to write to disk".format(time.time() - t0))

if __name__ == '__main__':

    f = GBTIDLFITSFile("filename.fits", config_file="gbtidlFitsConfig.json", loglevel=logging.DEBUG)
    f.create_data_hdu(10)
    # print(type(f[0]))
    # print(type(f[1]))
    # f.write_to_file()
    # from sdfits import generateBlankSDFits
    # a = generateBlankSDFits(10)
    # # print(f.data_hdu.data['DATA'][...])
    # col1 = a[1].data
    # col2 = f.data_hdu.data
    #
    # names1 = a[1].data.columns.names
    # names2 = f.data_hdu.data.columns.names
    #
    # names1.sort()
    # names2.sort()
    #
    # comp = [n1 == n2 for n1,n2 in zip(names1, names2)]
    # print(all(comp))
    # # compdata = [np.allclose(col1[n1][...],col2[n2][...]) for n1, n2 in zip(names1, names2)]
    # for n1, n2 in zip(names1, names2):
    #     try:
    #         equal = col1[n1].dtype == col2[n2].dtype
    #         print("Dtypes equal {} {}? {}".format(n1, n2,equal))
    #         if not equal:
    #             print(col1[n1].dtype, col2[n2].dtype)
    #     except Exception as err:
    #         print(err)

    # print(all(comp),all(compdata))
    # for name in f.primary_hdu.header.keys():
    #     print(name, f.primary_hdu.header[name]).
