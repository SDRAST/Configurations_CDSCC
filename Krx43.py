# -*- coding: utf-8 -*-
"""
Hardware configuration for Canberra 70-m 4-ch K-band with ROACHes

This describes the signal flow from the front ends through switches and
receivers to the signal processors.  It also identifies the specific
hardware used to implement generic devices.

Examples of use::
 In [8]:  station_configuration(roach_loglevel = Logging.logging.WARNING)
 Out[8]:
  (<Observatory.Observatory object at 0x30044d0>,
   DSS-21,
   {'noise': noise source, 'tone': synthesizer},
   {'RFI': noise with RFI, 'noise': pure noise, 'tone': pure tone},
   {'RFI': noise +RFI, 'noise': pure noise, 'tone': tone},
   {0: IF_sw 0, 1: IF_sw 1, 2: IF_sw 2, 3: IF_sw 3},
   {0: KurtSpec 0, 1: KurtSpec 1, 2: KurtSpec 2, 3: KurtSpec 3},
   {0: <Observatory.Instruments.synthesizers.Valon1 object at 0x3100650>,
    1: <Observatory.Instruments.synthesizers.Valon2 object at 0x31006d0>})
"""
import logging
module_logger = logging.getLogger(__name__)

from Observatory import Observatory, Telescope, FrontEnd, FE_channel, \
                        Receiver, Switch, Backend, BE_channel, Synthesizer, \
                        ClassInstance, DataChl, Observing_Device
try:
  from Observatory.BackEnds.ROACH.roach import Roach, Spec
except ImportError:
  module_logger.error("Cannot import Roach")

def station_configuration(roach_loglevel=logging.WARNING):
  """
  This is the test setup in the lab with simulated 'front ends' and
  'downconverters'::
   FE   FE_chl      RFMS       DC       BE
                             +----+
   K4   K4L1  ---------------|WBDC|-- ROACH1
                             |    |--
        K4R1  ---------------|WBDC|-- ROACH2
                             |    |--
        K4L2  ---------------|WBDC|-- ROACH3
                             |    |--
        K4R2  ---------------|WBDC|-- ROACH4
                             |    |
                             +----+
                             
  The superclasses, which are defined in module Observatory, are::
   - Observatory - site for the control room and signal processing equipment
   - Telescope   - which captures the radiation
   - FrontEnd    - which receives the radiation and splits it into its various
                   channels for further processing
   - FE_channel  - which processes on signal stream from the front end
   - Receiver    - which does the down-conversion for one channel
   - Backend     - which processes the down-converted signal from a channel
   - Switch      - which can change the routing of a signal
  When required, to specify monitor and control code, a superclass is
  subclassed with a device class.  Examples shown below are class
  KurtosisSpectrometer() for the BackEnd() superclass and JFW50MS287() for the
  Switch() superclass.
  
  @return: tuple with class instances for hardware
  """
  # specify the observing context
  lab = Observatory("Canberra K")
  telescope = Telescope(lab,dss=43)
  # specify the front ends; no actual M&C of hardware since DTO doesn't have
  # that option so no implementation class needed
  FE = {}
  FE["K4"] = FrontEnd(telescope,"new K")
  # define the front end channels; again, no actual M&C hardware
  FE_chl = {}
  FE_chl['K4A1']   = FE_channel(FE["K4"],"new K feed 1 pol A")
  FE_chl['K4B1']   = FE_channel(FE["K4"],"new K feed 1 pol B")
  FE_chl['K4A2']   = FE_channel(FE["K4"],"new K feed 2 pol A")
  FE_chl['K4B2']   = FE_channel(FE["K4"],"new K feed 2 pol B")
  # specify the down-converters and their signal sources; also not under our
  # control so no implementation classes
  DC = {}
  DC['WBDC1'] = Receiver(telescope,"WBDC feed 1 pol A")
  DC['WBDC2'] = Receiver(telescope,"WBDC feed 1 pol B")
  DC['WBDC3'] = Receiver(telescope,"WBDC feed 2 pol A")
  DC['WBDC4'] = Receiver(telescope,"WBDC feed 2 pol B")
  # specify where the DC inputs come from. There's no way to automate this.
  # These are single item lists because they are one-to-one connections
  FE_chl['K4A1'].destinations = [DC['WBDC1']]
  FE_chl['K4B1'].destinations = [DC['WBDC2']]
  FE_chl['K4A2'].destinations = [DC['WBDC3']]
  FE_chl['K4B2'].destinations = [DC['WBDC4']]
  for key in FE_chl.keys():
    module_logger.debug(" FE_chl[%s] connects to DC[%s]",
                        key, str(FE_chl[key].destinations))
  DC['WBDC1'].sources  = [FE_chl['K4A1']]
  DC['WBDC2'].sources  = [FE_chl['K4B1']]
  DC['WBDC3'].sources  = [FE_chl['K4A2']]
  DC['WBDC4'].sources  = [FE_chl['K4B2']]
  for key in DC.keys():
    module_logger.debug(" DC[%s] signal source is %s",
                   key,str(DC[key].sources))
  # The spectrometers require sample clock generators.
  sample_clk = {}
  sample_clk[0] = None
  sample_clk[1] = None
  sample_clk[2] = None
  sample_clk[3] = None
  #sample_clk[0] = ClassInstance(Synthesizer,Valon1,timeout=10)
  # Specify the backends; we need these before we can specify the switch
  # The back-end IDs are keyed to the switch outputs, that is, the first
  # switch output feeds spectrometer[0], the last spectrometer[3].
  roach = {}
  roaches = ['sao64k-1', 'sao64k-2', 'sao64k-3', 'sao64k-4'] # firmware.keys()
  roaches.sort()
  spec = {}
  data_channel = {}
  for name in roaches:
    module_logger.debug(' Instantiating %s', name)
    roach_index = int(name[-1]) - 1
    try:
      roach[roach_index] = ClassInstance(Backend,
                                         Spec,
                                         lab,
                                         key = None,
                                         roach=name,
                                         LO = sample_clk[roach_index],
                                         loglevel = roach_loglevel)
      spec[roach_index] = find_BE_channels(roach[roach_index])
      data_channel[roach_index] = {0: DataChl(roach[roach_index],
                                            'gbe0',
                                            'gpu1',
                                            10000)}
    except ImportError:
      module_logger.error("Cannot instantiate ROACH: %s", details)
      roach[roach_index] = Backend(lab,roaches[roach_index])
      spec[roach_index] = {0:{0:Observing_Device(roaches[roach_index])}}
      #data_channel[roach_index] = DataChl(roach[roach_index],"32K-1",None,None)
    except NameError, details:
      module_logger.error("Cannot instantiate ROACH: %s", details)
      roach[roach_index] = Backend(lab,roaches[roach_index])
      spec[roach_index] = {0:{0:Observing_Device(roaches[roach_index])}}
      #data_channel[roach_index] = DataChl(roach[roach_index],"32K-1",None,None)
  # Describe the backend input selector switches; real hardware this time
  # There is no IF switch at this time
  IFsw = {}
  # Summarize the signal flow
  for roach_index in spec.keys():
    for ADC_index in spec[roach_index].keys():
      for RF in spec[roach_index][0].keys():
        module_logger.debug(" spec[%d][0][%d] signal source is %s",
                   roach_index, RF, str(spec[roach_index][0][RF].sources) )
  return lab, telescope, FE, FE_chl, DC, IFsw, roach, spec, sample_clk
 
def find_BE_channels(roach):
  """
  Find the BE_channel instances associated with a Backend instance

  This assumes that BE_channel instances were created when a Spec
  instance is created.
  
  @param roach : Roach object
  @type  roach : Backend instance

  @return: dict
  """
  spec = {}
  module_logger.debug("find_BE_channels: finding spec[%d]",roach.number)
  ADC_keys = roach.ADC_inputs.keys()
  if roach.BE_channels:
    for ADC in ADC_keys:
      module_logger.debug("find_BE_channels: finding spec[%d][%d]",
                          roach.number,ADC)
      spec[ADC] = {}
      for RF in roach.ADC_inputs[ADC]:
        module_logger.debug("find_BE_channels: finding spec[%d][%d][%d]",
                            roach.number,ADC,RF)
        spec[ADC][RF] = roach.BE_channels[ADC][RF]
  module_logger.debug("find_BE_channels: Found instances: %s", str(spec))
  return spec