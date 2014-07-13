# -*- coding: utf-8 -*-
"""
Configuration data for the 4-ch K-band front-end and WBDC1
"""
import logging

from MonitorControl import Observatory, Telescope, ClassInstance
from MonitorControl.FrontEnds import FrontEnd
from MonitorControl.FrontEnds.K_band import K_4ch
from MonitorControl.Receivers import Receiver
from MonitorControl.Receivers.WBDC import WBDC1
from MonitorControl.BackEnds import Backend
from MonitorControl.BackEnds.ROACH1.SAOspec import SAOspec

module_logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG)

observatory = Observatory("Canberra")
telescope = Telescope(observatory, dss=43)
front_end = ClassInstance(FrontEnd, K_4ch, "K",
                     inputs = {'B1': telescope.outputs[telescope.name],
                               'B2': telescope.outputs[telescope.name]},
                     output_names = [['B1P1','B1P2'],
                                     ['B2P1','B2P2']])
receiver = ClassInstance(Receiver, WBDC1, "WBDC-1",
                     inputs = {'B1P1': front_end.outputs["B1P1"],
                               'B1P2': front_end.outputs["B1P2"],
                               'B2P1': front_end.outputs["B2P1"],
                               'B2P2': front_end.outputs["B2P2"]},
                     output_names = [['D1PAI1', 'D1PAI2'],
                                     ['D1PBI1', 'D1PBI2'],
                                     ['D2PAI1', 'D2PAI2'],
                                     ['D2PBI1', 'D2PBI2']])
back_end = ClassInstance(Backend, SAOspec, "SAO 32K",
                     inputs = {'SAO1': receiver.outputs['D1PAI1'],
                               'SAO2': receiver.outputs['D1PBI1'],
                               'SAO3': receiver.outputs['D2PAI1'],
                               'SAO4': receiver.outputs['D2PBI1']},
                     output_names = ['F1P1', 'F2P1', 'F1P2', 'F2P2'],
                     ROACHlist = ['roach1', 'roach2', 'roach3', 'roach4'])
back_end_list = [back_end]
