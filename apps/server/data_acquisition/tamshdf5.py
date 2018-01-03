"""
tamshdf5.py

A class for creating and writing HDF5 files that have the structure used in the TAMS project.

Uses a JSON configuration file.
"""
import os
import json
import logging
import datetime
import time

import numpy as np
import ephem
import h5py

from gbtidlfits import GBTIDLFITSFile

cur_dir = os.path.dirname(os.path.realpath(__file__))
default_config_file = os.path.join(cur_dir, "TAMSHDF5Config.json")

module_logger = logging.getLogger(__name__)

class TAMSHDF5File(object):

    def __init__(self, filename, config_file=default_config_file, **kwargs):
        """
        Create a hdf5 file with datasets used in TAMS.
        Args:
            filename (str):
            **kwargs: For support.logs.logging_config
        """
        self.logger = logging.getLogger(module_logger.name + ".TAMSHDF5File")
        with open(config_file, 'r') as f_config:
            self.config = json.load(f_config)

        if not os.path.exists(filename):
            self.logger.debug("File doesn't exist, creating in 'a' mode")
            mode = 'a'
        else:
            self.logger.debug("File already exists, opening in 'r+' mode")
            mode = 'r+'
        self.logger.debug("File to be opened in '{}' mode".format(mode))
        df = h5py.File(filename, mode)
        for name in self.config:
            dataset_config = self.config[name]
            shape = dataset_config.get('shape', None)
            maxshape = dataset_config.get('maxshape', None)
            dtype = dataset_config.get('dtype', None)
            if name in df:
                self.logger.debug("Data in TAMSHDF5 format already exists in this file.")
                break
            else:
                try:
                    df.create_dataset(name, shape=shape, maxshape=maxshape, dtype=dtype)
                    self.logger.debug("Adding {} to datafile. Shape: {}, maxshape: {}, dtype: {}".format(name, shape, maxshape, dtype))
                except RuntimeError:
                    # means we've already created this file
                    self.logger.debug("Unable to create requested dataset.")

        self.df = df
        self.filename = filename

    def __getitem__(self, item):
        """
        Act as if the open hdf5 file is the object.
        """
        return self.df[item]

    def __getattr__(self, item):
        """
        Act as if the open hdf5 file is the object in question.
        """
        return getattr(self.df, item)

    def convert_to_gbtidlfits(self, center_freq=21.49, thresh=22.0, outfile=None):
        """
        Convert to GBTIDL compatible FITS file.
        Args:
            center_freq (float): The center frequency
        Returns:
            GBTIDLFITSFile instance
        """
        ttotal = time.time()
        if not outfile:
            outfile = self.filename.strip("hdf5") + "fits"

        self.logger.info("Outfile: {}".format(outfile))

        lst = self.df['LST']
        src = self.df['source_name']
        rfreq = self.df['rest_freq']
        ofreq = self.df['obs_freq']
        velo = self.df['vsys']
        vref = self.df['v_ref']
        bw = self.df['bandwidth']
        scan_dur = self.df['scan_duration']
        expo = self.df['integ_time']
        pol = self.df['pol']
        timestmp = self.df['timestamp']
        fmode = self.df['mode']
        timeobs = self.df['time_obs']
        dateobs = self.df['date_obs']
        offsets = self.df['offsets']
        radec = self.df['source_radec']

        self.logger.info("Observation date: {}".format(dateobs[1][0]))
        self.logger.info("Observation time: {}".format(timeobs[1][0]))

        # Centre frequency, Velocity and CRPIX1:

        center_freq = (float(center_freq) / 10) * 1e10
        thresh = float(thresh)*1e9
        self.logger.debug("Calculated center frequency: {}".format(center_freq))
        self.logger.debug("Using CRPIX1 frequency: {}".format(thresh))
        if center_freq < thresh:
            crpix1 = -31127.929
        else:
            crpix1 = 31127.929

        self.logger.debug("Using CRPIX1: {}".format(crpix1))
        velo = velo[0][0]

        if velo < 1:
            velo = velo * 299792458
        else:
            velo = velo * 1e3

        # Convert source RA and Dec to the required format...
        radeg = np.rad2deg(ephem.hours(radec[0][0]))
        decdeg = np.rad2deg(ephem.degrees(radec[0][1]))

        self.logger.debug("Calculated ra and dec: {}, {}".format(radeg, decdeg))

        # Get LST in seconds...
        lst = lst[0][0]
        lstb = time.strptime(lst.split('.')[0], '%H:%M:%S')
        lsts = datetime.timedelta(hours=lstb.tm_hour, minutes=lstb.tm_min, seconds=lstb.tm_sec).total_seconds()
        self.logger.debug("Initial LST {}".format(lst))

        self.logger.info("Retrieved all keywords and observing parameters...")

        scans = self.df['scan_number'][...]
        tsys = self.df['Tsys'][...]

        t1 = time.time()
        spectrum1 = self.df['spectraCh1'][...]
        spectrum2 = self.df['spectraCh2'][...]
        spectrum3 = self.df['spectraCh3'][...]
        spectrum4 = self.df['spectraCh4'][...]
        self.logger.debug("Took {:.4f} seconds to get spectral data".format(time.time() - t1))

        azel = self.df['current_azel'][...]
        az = azel[:, 0]
        el = azel[:, 1]

        self.logger.debug("Rearranging power meter, az/el and spectra data...")

        t0 = time.time()
        nChannels = 4
        nSpectralBins = 32768

        # this business of setting different tsys values equal to each other seems weird.
        thresh = 22.0e9
        if center_freq < thresh:
            self.logger.debug(
                "Center Frequency is below {}, Setting tsys2 equal to tsys4, tsys3 equal to tsys1".format(thresh))
            tsys[:, 1] = np.copy(tsys[:, 3])
            tsys[:, 2] = np.copy(tsys[:, 0])
        if center_freq > thresh:
            self.logger.debug("Center Frequency is above {}, Setting tsys2 and tsys4 equal to tsys3, and tsys1 and tsys3 equal to tsys2.".format(thresh))
            tsys[:, 0] = np.copy(tsys[:, 1])
            tsys[:, 3] = np.copy(tsys[:, 2])
            tsys[:, 2] = np.copy(tsys[:, 0])
            tsys[:, 1] = np.copy(tsys[:, 3])

        n_scans = int(np.amax(scans)) # the number of scans that were taken in the observation
        # get the size of all the scans arrays, and find the minimum of this. We lose some information by doing this.
        scan_sizes = [scans[scans == i].shape[0] for i in xrange(1, 1 + n_scans)]
        min_size = np.amin(scan_sizes)
        n_rows = n_scans * min_size # the total number of rows per channel.

        scan_filter = np.array([np.where(scans == i)[0][:min_size] for i in xrange(1, 1 + int(n_scans))])
        scan_filter = scan_filter.reshape(n_rows)

        scan_flat = np.repeat(scans[scan_filter], nChannels)

        # original size of tsys (number of total records, nChannels). After filtering if becomes (n_rows, nChannels)
        tsys_filter = tsys[scan_filter]
        tsys_filter3d = tsys_filter.reshape((n_scans, min_size, nChannels)).swapaxes(1, 2)
        tsys_flat = tsys_filter3d.ravel()  # flatten the array

        az_filter = np.repeat(az[scan_filter], nChannels)
        az_filter3d = az_filter.reshape((n_scans, min_size, nChannels)).swapaxes(1, 2)
        az_flat = az_filter3d.ravel()

        el_filter = np.repeat(el[scan_filter], nChannels)
        el_filter3d = el_filter.reshape((n_scans, min_size, nChannels)).swapaxes(1, 2)
        el_flat = el_filter3d.ravel()

        spectrum1_filter = spectrum1[scan_filter]
        spectrum2_filter = spectrum2[scan_filter]
        spectrum3_filter = spectrum3[scan_filter]
        spectrum4_filter = spectrum4[scan_filter]

        spectra_filter = np.stack([spectrum1_filter,
                                   spectrum2_filter,
                                   spectrum3_filter,
                                   spectrum4_filter], axis=1)

        spectra_filter4d = spectra_filter.reshape((n_scans, min_size, nChannels, nSpectralBins)).swapaxes(1, 2)
        spectra_flat = spectra_filter4d.reshape((n_scans * min_size * nChannels, nSpectralBins))

        self.logger.info("Rearranging done. Took {:.4f} seconds".format(time.time() - t0))
        self.logger.info("Data prepared to be written to SDFITS file")
        self.logger.info("Generating GBTIDL keywords...")
        t0 = time.time()

        zero0 = np.zeros((min_size, 1))
        one1 = np.ones((min_size, 1))
        two2 = np.ones((min_size, 1)) * 2
        minus5 = np.ones((min_size, 1)) * -5.0
        minus6 = np.ones((min_size, 1)) * -6.0

        pl1scan = np.concatenate([zero0, zero0, one1, one1])
        fd1scan = np.concatenate([zero0, one1, zero0, one1])
        fe1scan = np.concatenate([one1, two2, one1, two2])
        c41scan = np.concatenate([minus5, minus5, minus6, minus6])
        ps1scan = np.concatenate([one1, one1, one1, one1])
        ps2scan = np.concatenate([two2, two2, two2, two2])
        pspair = np.append(ps1scan, ps2scan)

        fullpl = np.tile(pl1scan, n_scans).swapaxes(0, 1).ravel()
        fullfd = np.tile(fd1scan, n_scans).swapaxes(0, 1).ravel()
        fullfe = np.tile(fe1scan, n_scans).swapaxes(0, 1).ravel()
        fullc4 = np.tile(c41scan, n_scans).swapaxes(0, 1).ravel()
        fullps = np.tile(pspair, n_scans/2).ravel()

        self.logger.info("Done generating GBTIDL keywords. Took {:.4f} seconds".format(time.time() - t0))

        # Generate a blank SDFits file:

        f_fits = GBTIDLFITSFile(outfile)
        f_fits.create_data_hdu(spectra_flat.shape[0])

        sdtab = f_fits[-1].data

        self.logger.info("Populating keywords and writing data to SDFITS file...")
        t0 = time.time()
        sdtab["DATE-OBS"][:] = dateobs[0][0] + "T" + timeobs[0][0]
        sdtab["LST"][:] = lsts
        sdtab["OBSERVER"][:] = 'TAMS Team'
        sdtab["EXPOSURE"][:] = expo[0][0]
        sdtab["BANDWID"][:] = bw[0][0] * 1e6
        sdtab["RESTFREQ"][:] = rfreq[0][0]
        sdtab["OBSFREQ"][:] = center_freq
        sdtab["FREQRES"][:] = 37664.794
        sdtab["CDELT1"][:] = crpix1
        sdtab["CRVAL1"][:] = center_freq
        sdtab["CRPIX1"][:] = 16385
        sdtab["CTYPE1"][:] = 'FREQ-OBS'
        sdtab["CTYPE2"][:] = 'RA'
        sdtab["CTYPE3"][:] = 'DEC'
        sdtab["VELOCITY"][:] = velo
        sdtab["VFRAME"][:] = 0.0
        sdtab["VELDEF"][:] = 'OPTI-OBS'
        sdtab["IFNUM"][:] = 0
        sdtab["OBJECT"][:] = src[0][0]
        sdtab["OBSMODE"][:] = 'Nod:NONE:NONE'
        sdtab["PROCSIZE"][:] = 2.0
        sdtab["SIG"][:] = 'T'
        sdtab["CAL"][:] = 'F'
        sdtab["DURATION"][:] = expo[0][0]
        sdtab["FEEDEOFF"][:] = 0.0
        sdtab["FEEDXOFF"][:] = 0.0
        sdtab["CRVAL2"][:] = radeg
        sdtab["CRVAL3"][:] = decdeg
        sdtab["SITELONG"][:] = 211.019942862
        sdtab["SITELAT"][:] = -35.403983527
        sdtab["SITEELEV"][:] = 688.867
        sdtab['TSYS'][...] = tsys_flat
        sdtab["SCAN"][...] = scan_flat
        sdtab["FDNUM"][...] = fullfd
        sdtab["FEED"][...] = fullfe
        sdtab["PLNUM"][...] = fullpl
        sdtab["PROCSEQN"][...] = fullps
        sdtab["CRVAL4"][...] = fullc4
        sdtab["AZIMUTH"][...] = az_flat
        sdtab["ELEVATIO"][...] = el_flat
        sdtab['DATA'][:, 0, 0, 0, :] = spectra_flat # weird structure of DATA

        self.logger.info("Done populating SDFITS structure. Took {:.2f} seconds.".format(time.time() - t0))
        self.logger.info("Writing SDFITS file.")
        t0 = time.time()
        f_fits.write_to_file()

        self.logger.info("SDFITS file successfully written. Took {:.4f} seconds".format(time.time() - t0))
        self.logger.info("Total time converting to GBTIDL compatible file: {:.4f} seconds".format(time.time() - ttotal))
        return f_fits

if __name__ == '__main__':
    timestamp = datetime.datetime.utcnow().isoformat()
    # f = TAMSHDF5File("test-{}.hdf5".format(timestamp), loglevel=logging.DEBUG)
    # f = TAMSHDF5File("/home/dean/jpl-dsn/tamsfits/testData/hydraA.spec.hdf5", loglevel=logging.DEBUG)
    f = TAMSHDF5File("/home/dean/jpl-dsn/tamsfits/testData/A2LiP1I1472668425.41g1937044_603652s.spec.hdf5", loglevel=logging.DEBUG)
    f.convert_to_gbtidlfits(21.49)
    print("Flushing...")
    f.flush()
    print("Closing...")
    f.close()
