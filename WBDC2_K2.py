"""
Configuration for DSS-43 with front end K2 and receiver WBDC2
"""
import copy
import logging

from MonitorControl import ClassInstance, Device, Observatory, Telescope, Switch
from MonitorControl.BackEnds import Backend
from MonitorControl.BackEnds.ROACH1.SAOspec import SAOspec
from MonitorControl.Configurations.CDSCC.FO_patching import DistributionAssembly
from MonitorControl.FrontEnds import FrontEnd
from MonitorControl.FrontEnds.K_band import K_4ch
from MonitorControl.Receivers import Receiver
from MonitorControl.Receivers.WBDC.WBDC2 import WBDC2

logger = logging.getLogger(__name__)

class IFswitch(Device):
  """
  Ad hoc class to treat the manual patch panel as an IF switch.
  
  Naturally, this has nothing to control or monitor
  """
  def __init__(self, name, equipment, inputs=None, output_names=None):
    """
    Initialize the patch panel "switch"
    
    Gets the input port names from the receiver outputs
    """
    mylogger = logging.getLogger(logger.name+".IFswitch")
    da = DistributionAssembly()
    signals = da.get_inputs('ROACH1')
    if inputs == None:
      self.inputs = {}
      output_names = []
      for IF in range(4):
        output_name = "SAO"+str(IF+1)
        RF = signals[output_name]['RF']
        IF = signals[output_name]['IF']
        source_name = self._make_input_name(RF, IF)
        self.inputs[source_name] = equipment["Receiver"].outputs[source_name]
        output_names.append(output_name)
    else:
      self.inputs = inputs
    Device.__init__(self, name, inputs=self.inputs, output_names=output_names)
    innames = self.inputs.keys()
    innames.sort()
    for name in output_names:
      index = output_names.index(name)
      self.outputs[name].source = self.inputs[innames[index]]
      self.outputs[name].signal = copy.copy(self.outputs[name].source.signal)
    
  
  def _make_input_name(self, RF, IF):
    """
    """
    if RF[-1] == "E" or RF[-1] == "L":
      name = RF[:-1]+"P1"
    else:
      name = RF[:-1]+"P2"
    if IF == "L" or IF == "I":
      name += "I1"
    else:
      name += "I2"
    return name
  
def station_configuration(equipment, roach_loglevel=logging.WARNING):
  """
  Configuration for the K-band system on DSS-43 using WBDC2

  Feed 1 (F1) is at 024-0.016, F2 at 024+0.016.  The polarizations are linear,
  E and H.  There are so many receiver outputs that it is simpler to let the
  software generate them.
  """
  observatory = Observatory("Canberra")
  equipment['Telescope'] = Telescope(observatory, dss=43)
  telescope = equipment['Telescope']
  equipment['FrontEnd'] = ClassInstance(FrontEnd, K_4ch, "K", hardware=True,
                           inputs = {'KF1': telescope.outputs[telescope.name],
                                     'KF2': telescope.outputs[telescope.name]},
                           output_names = [['F1P1','F1P2'],
                                           ['F2P1','F2P2']])
  front_end = equipment['FrontEnd']
  equipment['Receiver'] = ClassInstance(Receiver, WBDC2, "WBDC-2",
                                        hardware=True,
                                  inputs = {'F1P1': front_end.outputs["F1P1"],
                                            'F1P2': front_end.outputs["F1P2"],
                                            'F2P1': front_end.outputs["F2P1"],
                                            'F2P2': front_end.outputs["F2P2"]})
  equipment['IF_switch'] = IFswitch("Patch Panel", equipment)
  patch_panel = equipment['IF_switch']
  equipment['Backend'] = ClassInstance(Backend, SAOspec, "SAO spectrometer",
                                inputs = {'SAO1': patch_panel.outputs['SAO1'],
                                          'SAO2': patch_panel.outputs['SAO2'],
                                          'SAO3': patch_panel.outputs['SAO3'],
                                          'SAO4': patch_panel.outputs['SAO4']})
  equipment['sampling_clock'] = None
  return observatory, equipment
