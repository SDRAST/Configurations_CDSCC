import json
import os
import datetime
import logging
import time

import astropy.units as u
import astropy.constants as constants
import ephem
import matplotlib.pyplot as plt
import numpy as np
import h5py

from support.Ephem import SerializableBody
from .gbtidlfits import GBTIDLFITSFile

module_logger = logging.getLogger(__name__)


class TAMSDataConsolidator(object):

    datetime_formatter = "%Y-%m-%dT%H:%M:%S.%f"
    reference_roach = "sao64k-1"

    def __init__(self):
        self._meta_data_file_path = ""
        self._backend_data_file_paths = {}
        self._antenna_data_file_path = ""
        self._frontend_data_file_path = ""
        self._receiver_data_file_path = ""

        self.timestamp = None
        self.meta_data = None

        self.obs_data = None

    @property
    def meta_data_file_path(self):
        return self._meta_data_file_path

    @meta_data_file_path.setter
    def meta_data_file_path(self, new_meta_data_file_path):
        self._meta_data_file_path = new_meta_data_file_path
        self.load_meta_data()

    @property
    def backend_data_file_paths(self):
        return self._backend_data_file_paths

    @property
    def antenna_data_file_path(self):
        return self._antenna_data_file_path

    @property
    def frontend_data_file_path(self):
        return self._frontend_data_file_path

    @property
    def receiver_data_file_path(self):
        return self._receiver_data_file_path

    @property
    def tsys_factors(self):
        if "tsys_factors" in self.meta_data:
            return np.array(self.meta_data["tsys_factors"])
        else:
            module_logger.error(
                ("TAMSDataConsolidator.tsys_factors: "
                 "No tsys factors found in meta data file")
            )
            return np.ones(4)

    def load_meta_data(self):
        """
        """
        t0 = time.time()

        def correct_path(original_path, meta_data_dir):
            """
            Paths is meta data files are absolute paths on based on the box
            where they were created. This function corrects for this.

            Args:
                original_path (str):
                meta_data_dir (str):
            Returns:
                str:
            """
            if original_path is not None:
                file_name = os.path.basename(original_path)
                return os.path.join(meta_data_dir, file_name)
            else:
                return None

        def get_timestamp_from_file_path(file_path):
            timestamp_str = os.path.basename(file_path).split(".")[1]
            timestamp = datetime.datetime.strptime(
                timestamp_str, "%Y-%j-%H%M%S"
            )
            return timestamp

        self.timestamp = get_timestamp_from_file_path(self.meta_data_file_path)

        with open(self.meta_data_file_path, "r") as f:
            meta_data = json.load(f)
        meta_data_dir = os.path.dirname(self.meta_data_file_path)

        self._antenna_data_file_path = correct_path(
            meta_data.get("Antenna", None), meta_data_dir)
        self._receiver_data_file_path = correct_path(
            meta_data.get("Receiver", None), meta_data_dir)
        self._frontend_data_file_path = correct_path(
            meta_data.get("FrontEnd", None), meta_data_dir)

        all_data_files = os.listdir(meta_data_dir)

        for roach_name in meta_data["Backend"]:
            possible_roach_data_files = []
            for data_file_path in all_data_files:
                if roach_name in data_file_path:
                    possible_roach_data_files.append(
                        correct_path(data_file_path, meta_data_dir)
                    )
            self._backend_data_file_paths[roach_name] = \
                possible_roach_data_files

        module_logger.debug(
            ("TAMSDataConsolidator.load_meta_data: "
             "Took {:.4f} seconds to load meta data").format(time.time() - t0)
        )

        self.meta_data = meta_data
        return self.meta_data

    def _load_antenna_obs_data(self):

        nat = np.datetime64("NaT")
        antenna_obs_data = {}
        with h5py.File(self.antenna_data_file_path, "r") as antenna_f:
            timestamp = (antenna_f["timestamp"][...]
                         .astype("datetime64")
                         .reshape(-1))
            nat_mask = timestamp != nat
            antenna_obs_data["az"] = \
                antenna_f["AzimuthAngle"][...][nat_mask]
            antenna_obs_data["el"] = \
                antenna_f["ElevationAngle"][...][nat_mask]
            antenna_obs_data["el_offset"] = \
                antenna_f["ElevationPositionOffset"][...][nat_mask]
            antenna_obs_data["xel_offset"] = \
                antenna_f["CrossElevationPositionOffset"][...][nat_mask]
            antenna_obs_data["timestamp"] = timestamp[nat_mask]

        return antenna_obs_data

    def _load_frontend_obs_data(self):

        nat = np.datetime64("NaT")
        frontend_obs_data = {}
        with h5py.File(self.frontend_data_file_path, "r") as frontend_f:
            timestamp = (frontend_f["timestamp"][...]
                         .astype("datetime64")
                         .reshape(-1,))
            nat_mask = timestamp != nat
            pm_readings = frontend_f["pm_readings"][...][nat_mask]
            tsys = self.tsys_factors * pm_readings
            frontend_obs_data["tsys"] = tsys
            frontend_obs_data["timestamp"] = timestamp[nat_mask]
        return frontend_obs_data

    def _load_receiver_obs_data(self):

        receiver_obs_data = {}
        if self.receiver_data_file_path is not None:
            with h5py.File(self.receiver_data_file_path, "r") as receiver_f:
                pass

        return receiver_obs_data

    def _load_backend_obs_data(self):
        """
        """
        nat = np.datetime64("NaT")
        spectra = {}
        loaders = {
            "timestamp": lambda f_obj, scan:\
                f_obj[scan]["timestamp"][...].astype("datetime64").reshape(-1),
            "accum": lambda f_obj, scan: \
                f_obj[scan]["data"][...],
            "accum_number": lambda f_obj, scan: \
                f_obj[scan]["accumulation_number"][...].reshape(-1),
            "scan": lambda f_obj, scan: \
                int(scan) * np.ones(f_obj[scan]["data"].shape[0])
        }
        for roach_name in self.backend_data_file_paths:
            roach_data_file_paths = self.backend_data_file_paths[roach_name]
            spectra[roach_name] = {}
            for file_path in roach_data_file_paths:
                with h5py.File(file_path, "r") as roach_f:
                    for scan in roach_f:
                        for field in loaders:
                            loader = loaders[field]
                            arr = loader(roach_f, scan)
                            if field not in spectra[roach_name]:
                                spectra[roach_name][field] = arr
                            else:
                                spectra[roach_name][field] = np.concatenate(
                                    (spectra[roach_name][field], arr)
                                )
            nat_mask = spectra[roach_name]["timestamp"] != nat
            for field in loaders:
                spectra[roach_name][field] = \
                    spectra[roach_name][field][nat_mask]
            ordered_idx = np.argsort(spectra[roach_name]["timestamp"])
            for field in loaders:
                spectra[roach_name][field] = \
                    spectra[roach_name][field][ordered_idx]
        return spectra

    def load_obs_data(self):
        """
        Load data from HDF5 files
        """

        t0 = time.time()
        nat = np.datetime64("NaT")
        obs_data = {}

        obs_data["Antenna"] = self._load_antenna_obs_data()
        obs_data["FrontEnd"] = self._load_frontend_obs_data()
        obs_data["Receiver"] = self._load_receiver_obs_data()
        obs_data["Backend"] = self._load_backend_obs_data()


        def reconcile_timestamps(reference, target):
            """
            Using one of the ROACHs as reference, we have to get temporally
            corresponding data rows from Antenna, frontend, and  receiver.
            Args:
                reference (np.ndarray): reference roach timestamp array
                target (np.ndarray): antenna, frontend, or receiver timestamp array
            Returns:
                np.ndarray
            """
            # reference = reference[np.logical_and(
            #     reference >= target[0], reference <= target[-1]
            # )]
            reference_tiled = np.tile(reference, (target.shape[0], 1)).transpose()
            delta_abs = np.abs(reference_tiled - target)
            reconcile_args = np.argmin(delta_abs, axis=1)
            return reconcile_args

        reference = obs_data["Backend"][self.reference_roach]["timestamp"]

        # target = obs_data["Antenna"]["timestamp"]
        # frontend_idx = reconcile_timestamps(reference, target)
        # fig, ax = plt.subplots(1,1)
        # plot_kwargs = dict(linestyle="None", marker="o")
        # reference = reference[reference >= target[0]]
        # ax.plot(reference, label="sao64k-1", **plot_kwargs)
        # ax.plot(target, label="frontend", **plot_kwargs)
        # for idx in frontend_idx:
        #     hline = np.repeat(target[idx], target.shape[0])
        #     ax.plot(hline, color="k")
        # # print(target[frontend_idx[0]])
        # ax.legend()
        # ax.set_title(frontend_idx)
        # plt.show()

        antenna_timestamp = obs_data["Antenna"]["timestamp"]
        for roach_name in obs_data["Backend"]:
            timestamp = obs_data["Backend"][roach_name]["timestamp"]
            timestamp_idx = np.logical_and(
                timestamp >= antenna_timestamp[0],
                timestamp <= antenna_timestamp[-1]
            )
            for field in obs_data["Backend"][roach_name]:
                obs_data["Backend"][roach_name][field] = \
                    obs_data["Backend"][roach_name][field][timestamp_idx]

        equip = (e for e in obs_data if e != "Backend")
        for e in equip:
            if "timestamp" not in obs_data[e]:
                continue
            target = obs_data[e]["timestamp"]
            if target.shape[0] == 0:
                continue
            idx = reconcile_timestamps(reference, target)
            for field in obs_data[e]:
                obs_data[e][field] = obs_data[e][field][idx]

        self.obs_data = obs_data
        module_logger.debug(
            ("TAMSDataConsolidator.load_obs_data: "
             "Took {:.4f} seconds to load obs data".format(time.time() - t0))
        )
        return self.obs_data

    def _calculate_lst(self, observer, timestamp):
        """
        Calculate LST for
        """
        observer.date = timestamp
        return observer.sidereal_time()

    def _calculate_source_radec(self, source, timestamp):
        """

        """
        if isinstance(timestamp, np.datetime64):
            timestamp = datetime.datetime.strptime(
                str(timestamp), self.datetime_formatter
            )
        source.compute(timestamp)
        radec = [source.ra, source.dec]
        return [np.rad2deg(coord) for coord in radec]

    def _calculate_obs_freq(self, ref_freq, obs_velocity):
        """
        Args:
            ref_freq (astropy.Quantity): reference frequency
            obs_velocity (astropy.Quantity): observed velocity
        Returns:
            astropy.Quantity: obs_velocity in frequency
        """
        return (ref_freq / (1. + (obs_velocity/constants.c))).to("GHz")

    def dump_gbtidlfits(self,
                        center_freq=21.49,
                        thresh=22.0,
                        outfile=None):
        """
        dump a GBTIDL compatible FITS file.
        Args:
            center_freq (float): The center frequency
        Returns:
            GBTIDLFITSFile instance
        """
        if self.obs_data is None or self.meta_data is None:
            module_logger.error(
                ("TAMSDataConsolidator.dump_gbtidlfits: "
                 "Need to call load_meta_data and load_obs_data before calling "
                 "this method")
            )
            return

        ttotal = time.time()

        if outfile is None:
            meta_data_dir = os.path.dirname(self.meta_data_file_path)
            timestamp_str = os.path.basename(
                self.meta_data_file_path).split(".")[1]

            outfile = os.path.join(
                meta_data_dir, "out.{}.fits".format(timestamp_str))

        module_logger.info("TAMSDataConsolidator.dump_gbtidlfits: Outfile: {}".format(outfile))

        source = SerializableBody.from_dict(self.meta_data["source"])
        observer = source.get_observer()
        timestamp = self.obs_data["Backend"][self.reference_roach]["timestamp"]
        initial_dt = datetime.datetime.strptime(
            str(timestamp[0]), self.datetime_formatter
        )

        ra, dec = self._calculate_source_radec(source, initial_dt)
        lst = datetime.datetime.strptime(
            str(self._calculate_lst(observer, initial_dt)),
            "%H:%M:%S.%f"
        )
        lst_seconds = datetime.timedelta(
            hours=lst.hour,
            minutes=lst.minute,
            seconds=lst.second,
            microseconds=lst.microsecond
        ).total_seconds()

        rest_freq = self.meta_data["rest_freq"] * u.GHz
        source_velo = source.info["velocity"] * (u.km / u.s)
        obs_freq = self._calculate_obs_freq(
            rest_freq, source_velo
        ).value

        bandwidth = (self.meta_data["Backend"]
                     [self.reference_roach]
                     ["summary"]
                     ["bandwidth"]) * u.MHz

        # lst = self.df['LST']
        # src = self.df['source_name']
        # rfreq = self.df['rest_freq']
        # ofreq = self.df['obs_freq']
        # velo = self.df['vsys']
        # vref = self.df['v_ref'] # OPTI-LSR
        # bw = self.df['bandwidth']
        # scan_dur = self.df['scan_duration']
        # expo = self.df['integ_time']
        # pol = self.df['pol']
        # timestmp = self.df['timestamp']
        # fmode = self.df['mode']
        # timeobs = self.df['time_obs']
        # dateobs = self.df['date_obs']
        # offsets = self.df['offsets']
        # radec = self.df['source_radec']

        # module_logger.info("TAMSDataConsolidator.dump_gbtidlfits: Observation date: {}".format(dateobs[1][0]))
        # module_logger.info("TAMSDataConsolidator.dump_gbtidlfits: Observation time: {}".format(timeobs[1][0]))

        # Centre frequency, Velocity and CRPIX1:

        center_freq = (float(center_freq) / 10) * 1e10
        thresh = float(thresh)*1e9
        module_logger.debug(
            ("TAMSDataConsolidator.dump_gbtidlfits: "
             "Calculated center frequency: {}".format(center_freq)))
        module_logger.debug(
            ("TAMSDataConsolidator.dump_gbtidlfits: "
             "Using CRPIX1 frequency: {}".format(thresh)))
        if center_freq < thresh:
            crpix1 = -31127.929
        else:
            crpix1 = 31127.929

        module_logger.debug(
            ("TAMSDataConsolidator.dump_gbtidlfits: "
             "Using CRPIX1: {}".format(crpix1)))

        # velo = velo[0][0]

        # we want velocity in m/s
        # if velo < 1:
        #     velo = velo * 299792458
        # else:
        #     velo = velo * 1e3

        # Convert source RA and Dec to the required format...
        # ra = np.rad2deg(ephem.hours(radec[0][0]))
        # dec = np.rad2deg(ephem.degrees(radec[0][1]))

        module_logger.debug("TAMSDataConsolidator.dump_gbtidlfits: Calculated ra and dec: {}, {}".format(ra, dec))

        # Get LST in seconds...
        # lst = lst[0][0]
        # lstb = time.strptime(lst.split('.')[0], '%H:%M:%S')
        # lsts = datetime.timedelta(hours=lstb.tm_hour, minutes=lstb.tm_min, seconds=lstb.tm_sec).total_seconds()
        # module_logger.debug("TAMSDataConsolidator.dump_gbtidlfits: Initial LST {}".format(lst))

        scans = self.obs_data["Backend"][self.reference_roach]["scan"]
        tsys = self.obs_data["FrontEnd"]["tsys"]

        # scans = self.df['scan_number'][...]
        # tsys = self.df['Tsys'][...]

        t1 = time.time()
        spectra_roach1 = self.obs_data["Backend"]["sao64k-1"]["accum"]
        spectra_roach2 = self.obs_data["Backend"]["sao64k-1"]["accum"]
        spectra_roach3 = self.obs_data["Backend"]["sao64k-1"]["accum"]
        spectra_roach4 = self.obs_data["Backend"]["sao64k-1"]["accum"]
        # spectra_roach1 = self.df['spectraCh1'][...]
        # spectra_roach2 = self.df['spectraCh2'][...]
        # spectra_roach3 = self.df['spectraCh3'][...]
        # spectra_roach4 = self.df['spectraCh4'][...]
        module_logger.debug("TAMSDataConsolidator.dump_gbtidlfits: Took {:.4f} seconds to get spectral data".format(time.time() - t1))

        az = self.obs_data["Antenna"]["az"]
        el = self.obs_data["Antenna"]["el"]
        # azel = self.df['current_azel'][...]
        # az = azel[:, 0]
        # el = azel[:, 1]

        module_logger.debug("TAMSDataConsolidator.dump_gbtidlfits: Rearranging power meter, az/el and spectra data...")

        t0 = time.time()
        n_channels = len(self.obs_data["Backend"])
        n_spectral_bins = self.meta_data["Backend"][self.reference_roach]["summary"]["nchans"]

        # this business of setting different tsys values equal to each other seems weird.
        thresh = 22.0e9
        if center_freq < thresh:
            module_logger.debug(
                ("TAMSDataConsolidator.dump_gbtidlfits: "
                 "Center Frequency is below {}, Setting tsys2 equal to tsys4, "
                 "tsys3 equal to tsys1".format(thresh)))
            tsys[:, 1] = np.copy(tsys[:, 3])
            tsys[:, 2] = np.copy(tsys[:, 0])
        if center_freq > thresh:
            module_logger.debug(
                ("TAMSDataConsolidator.dump_gbtidlfits: "
                 "Center Frequency is above {}, Setting tsys2 and tsys4 "
                 "equal to tsys3, and tsys1 "
                 "and tsys3 equal to tsys2.".format(thresh)))
            tsys[:, 0] = np.copy(tsys[:, 1])
            tsys[:, 3] = np.copy(tsys[:, 2])
            tsys[:, 2] = np.copy(tsys[:, 0])
            tsys[:, 1] = np.copy(tsys[:, 3])

        n_scans = int(np.amax(scans)) # the number of scans that were taken in the observation
        # get the size of all the scans arrays, and find the minimum of this. We lose some information by doing this.
        scan_sizes = [scans[scans == i].shape[0]
                      for i in xrange(1, 1 + n_scans)]
        min_size = np.amin(scan_sizes)
        n_rows = n_scans * min_size # the total number of rows per channel.

        scan_filter = np.array([np.where(scans == i)[0][:min_size]
                                for i in xrange(1, 1 + int(n_scans))])
        scan_filter = scan_filter.reshape(n_rows)

        scan_flat = np.repeat(scans[scan_filter], n_channels)

        # original size of tsys (number of total records, n_channels). After filtering if becomes (n_rows, n_channels)
        tsys_filter = tsys[scan_filter]
        tsys_filter3d = tsys_filter.reshape(
            (n_scans, min_size, n_channels)).swapaxes(1, 2)
        tsys_flat = tsys_filter3d.ravel()  # flatten the array

        az_filter = np.repeat(az[scan_filter], n_channels)
        az_filter3d = az_filter.reshape(
            (n_scans, min_size, n_channels)).swapaxes(1, 2)
        az_flat = az_filter3d.ravel()

        el_filter = np.repeat(el[scan_filter], n_channels)
        el_filter3d = el_filter.reshape(
            (n_scans, min_size, n_channels)).swapaxes(1, 2)
        el_flat = el_filter3d.ravel()

        spectra_roach1_filter = spectra_roach1[scan_filter]
        spectra_roach2_filter = spectra_roach2[scan_filter]
        spectra_roach3_filter = spectra_roach3[scan_filter]
        spectra_roach4_filter = spectra_roach4[scan_filter]

        spectra_filter = np.stack([spectra_roach1_filter,
                                   spectra_roach2_filter,
                                   spectra_roach3_filter,
                                   spectra_roach4_filter], axis=1)

        spectra_filter4d = spectra_filter.reshape(
            (n_scans, min_size, n_channels, n_spectral_bins)).swapaxes(1, 2)
        spectra_flat = spectra_filter4d.reshape(
            (n_scans * min_size * n_channels, n_spectral_bins))

        module_logger.info(
            ("TAMSDataConsolidator.dump_gbtidlfits: "
             "Rearranging done. Took {:.4f} seconds".format(time.time() - t0)))
        module_logger.info(
            ("TAMSDataConsolidator.dump_gbtidlfits: "
             "Data prepared to be written to SDFITS file"))
        module_logger.info(
            ("TAMSDataConsolidator.dump_gbtidlfits: "
             "Generating GBTIDL keywords..."))
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

        module_logger.info(
            ("TAMSDataConsolidator.dump_gbtidlfits: "
             "Done generating GBTIDL keywords. "
             "Took {:.4f} seconds".format(time.time() - t0)))

        # Generate a blank SDFits file:

        f_fits = GBTIDLFITSFile(outfile)
        f_fits.create_data_hdu(spectra_flat.shape[0])

        sdtab = f_fits[-1].data

        module_logger.info(
            ("TAMSDataConsolidator.dump_gbtidlfits: "
             "Populating keywords and writing data to SDFITS file..."))
        t0 = time.time()
        sdtab["DATE-OBS"][:] = str(initial_dt)
        sdtab["LST"][:] = lst_seconds
        sdtab["OBSERVER"][:] = 'TAMS Team'
        sdtab["EXPOSURE"][:] = self.meta_data["integration_time"]
        sdtab["BANDWID"][:] = bandwidth.value
        sdtab["RESTFREQ"][:] = rest_freq.to("MHz").value
        sdtab["OBSFREQ"][:] = center_freq
        sdtab["FREQRES"][:] = 37664.794
        sdtab["CDELT1"][:] = crpix1
        sdtab["CRVAL1"][:] = center_freq
        sdtab["CRPIX1"][:] = 16385
        sdtab["CTYPE1"][:] = 'FREQ-OBS'
        sdtab["CTYPE2"][:] = 'RA'
        sdtab["CTYPE3"][:] = 'DEC'
        sdtab["VELOCITY"][:] = source_velo.to("m/s").value
        sdtab["VFRAME"][:] = 0.0
        sdtab["VELDEF"][:] = 'OPTI-OBS'
        sdtab["IFNUM"][:] = 0
        sdtab["OBJECT"][:] = source.name
        sdtab["OBSMODE"][:] = 'Nod:NONE:NONE'
        sdtab["PROCSIZE"][:] = 2.0
        sdtab["SIG"][:] = 'T'
        sdtab["CAL"][:] = 'F'
        sdtab["DURATION"][:] = self.meta_data["integration_time"]
        sdtab["FEEDEOFF"][:] = 0.0
        sdtab["FEEDXOFF"][:] = 0.0
        sdtab["CRVAL2"][:] = ra
        sdtab["CRVAL3"][:] = dec
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

        module_logger.info(
            ("TAMSDataConsolidator.dump_gbtidlfits: "
             "Done populating SDFITS structure. "
             "Took {:.2f} seconds.".format(time.time() - t0)))
        module_logger.info(
            ("TAMSDataConsolidator.dump_gbtidlfits: "
             "Writing SDFITS file."))
        t0 = time.time()
        f_fits.write_to_file()

        module_logger.info(
            ("TAMSDataConsolidator.dump_gbtidlfits: "
             "SDFITS file successfully written. "
             "Took {:.4f} seconds".format(time.time() - t0)))
        module_logger.info(
            ("TAMSDataConsolidator.dump_gbtidlfits: "
             "Total time converting to GBTIDL compatible file: "
             "{:.4f} seconds".format(time.time() - ttotal)))
        return f_fits
