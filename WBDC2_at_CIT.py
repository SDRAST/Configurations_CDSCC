from MonitorControl import Observatory, Telescope, ClassInstance
from MonitorControl.FrontEnds import FrontEnd
from MonitorControl.FrontEnds.K_band import K_4ch
from MonitorControl.Receivers import Receiver
from MonitorControl.Receivers.WBDC.WBDC2 import WBDC2

import logging
module_logger = logging.getLogger(__name__)

def station_configuration(equipment, roach_loglevel=logging.WARNING):
  """
  Configuration for the K-band system on DSS-43 using WBDC2

  Feed 1 (F1) is at 024-0.016, F2 at 024+0.016.  The polarizations are linear,
  E and H.  There are so many receiver outputs that it is simpler to let the
  software generate them.
  """
  observatory = Observatory("Canberra")
  telescope = Telescope(observatory, dss=43)
  front_end = ClassInstance(FrontEnd, K_4ch, "K",
                     inputs = {'KF1': telescope.outputs[telescope.name],
                               'KF2': telescope.outputs[telescope.name]},
                     output_names = [['F1E','F1H'],
                                     ['F2E','F2H']])
  IFswitch = None
  receiver = ClassInstance(Receiver, WBDC2, "WBDC-2",
                           inputs = {'F1E': front_end.outputs["F1E"],
                                     'F1H': front_end.outputs["F1H"],
                                     'F2E': front_end.outputs["F2E"],
                                     'F2H': front_end.outputs["F2H"]})
  clock = None,
  backend = None
  return observatory, telescope, front_end, receiver, IFswitch, clock, backend
