"""
Configuration for DSS-43 with front end DSN X and SAO backend
"""
import copy
import logging

from MonitorControl import ClassInstance, Device, Observatory, Telescope, Switch
from MonitorControl.BackEnds import Backend
from MonitorControl.BackEnds.ROACH1.SAOspec import SAOspec
from MonitorControl.Configurations.CDSCC.FO_patching import DistributionAssembly
from MonitorControl.FrontEnds import FrontEnd
from MonitorControl.FrontEnds.DSN import DSN_fe
from MonitorControl.Receivers import Receiver
from MonitorControl.Receivers.DSN import DSN_rx


logger = logging.getLogger(__name__)
  
def station_configuration(equipment, roach_loglevel=logging.WARNING):
  """
  Configuration for the DSN X-band system on DSS-43 using SAO backend

  Feed 1 (F1) is at 024-0.016, F2 at 024+0.016.  The polarizations are linear,
  E and H.  There are so many receiver outputs that it is simpler to let the
  software generate them.
  """
  observatory = Observatory("Canberra")
  equipment['Telescope'] = Telescope(observatory, dss=43)
  telescope = equipment['Telescope']
  equipment['FrontEnd'] = ClassInstance(FrontEnd, DSN_fe, "X43",
                           inputs = {'X43': telescope.outputs[telescope.name]},
                           output_names = ['XR','XL'])
  front_end = equipment['FrontEnd']
  equipment['Receiver'] = ClassInstance(Receiver, DSN_rx, "X",
                                  inputs = {'XR': front_end.outputs["XR"],
                                            'XL': front_end.outputs["XL"]})
  equipment['Backend'] = ClassInstance(Backend, SAOspec, "SAO spectrometer",
                                       hardware = False,
                                inputs = {'SAO1': equipment['Receiver'].outputs['XRU'],
                                          'SAO2': None,
                                          'SAO3': equipment['Receiver'].outputs['XLU'],
                                          'SAO4': None})
  equipment['sampling_clock'] = None
  return observatory, equipment
