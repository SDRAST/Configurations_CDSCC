from MonitorControl.Configurations.configCDSCC.K_4ch import \
  observatory as obs, \
  telescope as tel, \
  front_end as fe, \
  receiver as rx, \
  back_end_list as BE_list
from MonitorControl.SDFITS import FITSfile
#from MonitorControl.config_test import *

import pyfits

if __name__ == "__main__":
  logging.basicConfig(level=logging.DEBUG)
  testlogger = logging.getLogger()
  testlogger.setLevel(logging.DEBUG)
  
  rx.set_IF_mode(SB_separated=True)

  FITS_obj =  FITSfile(tel)
  FITS_obj.maketables(tel, BE_list)
  hdulist = pyfits.HDUList([FITS_obj.prihdu]+FITS_obj.tables)
