"""
Testing just through the WBDC
"""
# For logging control only:
#import Electronics
#import Electronics.Interfaces
#import Electronics.Interfaces.LabJack
# Referenced here
from MonitorControl import Observatory, Telescope, ClassInstance
from MonitorControl.FrontEnds import FrontEnd
from MonitorControl.FrontEnds.K_band import K_4ch
from MonitorControl.Receivers import Receiver
from MonitorControl.Receivers.WBDC.WBDC2 import WBDC2
from MonitorControl.config_test import *
from support import logs

def config():
  """
  Configuration for the K-band system on DSS-43 using WBDC2

  Feed 1 (F1) is at 024-0.016, F2 at 024+0.016.  The polarizations are linear,
  X and Y.  There are so many receiver outputs that it is simpler to let the
  software generate them.
  """
  observatory = Observatory("Canberra")
  telescope = Telescope(observatory, dss=43)
  front_end = ClassInstance(FrontEnd, K_4ch, "K",
                     inputs = {'KF1': telescope.outputs[telescope.name],
                               'KF2': telescope.outputs[telescope.name]},
                     output_names = [['F1X','F1Y'],
                                     ['F2X','F2Y']])
  receiver = ClassInstance(Receiver, WBDC2, "WBDC-2",
                     inputs = {'F1X': front_end.outputs["F1X"],
                               'F1Y': front_end.outputs["F1Y"],
                               'F2X': front_end.outputs["F2X"],
                               'F2Y': front_end.outputs["F2Y"]})
  return observatory, telescope, front_end, receiver
                                     
if __name__ == "__main__":
  logging.basicConfig(level=logging.DEBUG)
  #loggers = logs.set_module_loggers({"Electronics.Interfaces.LabJack": "debug",
  #                                   "MonitorControl.Receivers": "debug"})
  testlogger = logging.getLogger()
  logs.init_logging(testlogger, loglevel=logging.DEBUG, consolevel=logging.DEBUG)

  observatory, telescope, front_end, receiver = config()
  receiver.set_IF_mode(SB_separated=True)
