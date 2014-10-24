"""
Testing just through the WBDC
"""
from MonitorControl import Observatory, Telescope, ClassInstance
from MonitorControl.FrontEnds import FrontEnd
from MonitorControl.FrontEnds.K_band import K_4ch
from MonitorControl.Receivers import Receiver
from MonitorControl.Receivers.WBDC.WBDC2 import WBDC2
from MonitorControl.config_test import *

def config():
  observatory = Observatory("Canberra")
  telescope = Telescope(observatory, dss=43)
  front_end = ClassInstance(FrontEnd, K_4ch, "K",
                     inputs = {'B1': telescope.outputs[telescope.name],
                               'B2': telescope.outputs[telescope.name]},
                     output_names = [['B1P1','B1P2'],
                                     ['B2P1','B2P2']])
  receiver = ClassInstance(Receiver, WBDC2, "WBDC-2",
                     inputs = {'B1P1': front_end.outputs["B1P1"],
                               'B1P2': front_end.outputs["B1P2"],
                               'B2P1': front_end.outputs["B2P1"],
                               'B2P2': front_end.outputs["B2P2"]})
  return observatory, telescope, front_end, receiver
                                     
if __name__ == "__main__":
  logging.basicConfig(level=logging.DEBUG)
  testlogger = logging.getLogger()
  testlogger.setLevel(logging.DEBUG)
  observatory, telescope, front_end, receiver = config()
  receiver.set_IF_mode(SB_separated=True)
