"""
Module to read F/O patching spreadsheet

This depends on the spreadsheet having the following format::
  Row  1 - Column name
  Row  2 - Selected patch; all blank except for the cell of the selected patch
  Row  3 - Band 18, Receiver 1, Pol E, IF L
  ...
  Row 42 - Band 26, Receiver 2, Pol H, IF U
"""
import logging
from openpyxl import load_workbook
from openpyxl.reader.excel import InvalidFileException
from support.excel import *

module_logger = logging.getLogger(__name__)

repo_path = "/usr/local/lib/python2.7/DSN-Sci-packages/"
modulepath = repo_path+"MonitorControl/Configurations/CDSCC/"
paramfile = "FO_patching.xlsx"

label_map = {"E": 1, "H": 2, "L": 1, "U": 2}

class DistributionAssembly(object):
  """
  """
  def __init__(self, parampath=modulepath, paramfile=paramfile):
    """
    Create an instance of FirmwareServer()
    """
    self.parampath = parampath
    self.paramfile = paramfile
    self.logger = logging.getLogger(module_logger.name+".DistributionAssembly")
    self._open_patchpanel_spreadsheet()
    self.patching = self.get_patching()

  def _open_patchpanel_spreadsheet(self):
    """
    Get the firmware summary worksheet
    """
    self.logger.debug("_open_patchpanel_spreadsheet: for %s",
      self.parampath+self.paramfile)
    try:
      self.workbook = load_workbook(self.parampath+self.paramfile)
    except IOError, details:
      self.logger.error(
      "_open_patchpanel_spreadsheet: loading spreadsheet failed with IO error.",
                        exc_info=True)
      raise IOError
    except InvalidFileException:
      self.logger.error(
      "_open_patchpanel_spreadsheet: .reader.excel doesn't like this file.",
                        exc_info=True)
      raise InvalidFileException
    except AttributeError:
      self.logger.error(
                        "_open_patchpanel_spreadsheet: attribute error.",
                        exc_info=True)
      raise AttributeError
    self.sheet_names = self.workbook.get_sheet_names()
    self.sheet_names.sort()
    self.logger.debug("_open_patchpanel_spreadsheet: sheet names: %s",
                      str(self.sheet_names))
    self.worksheet = self.workbook.get_sheet_by_name(self.sheet_names[-1])
    column_names = get_column_names(self.worksheet)
    self.logger.debug("_open_patchpanel_spreadsheet: columns found in %s:",
                      self.sheet_names[-1])
    for name in column_names.keys():
      if column_names[name]:
        self.logger.debug("_open_patchpanel_spreadsheet: %s: %s",
                          name,column_names[name])
    self.current_patch()
    self.logger.debug("_open_patchpanel_spreadsheet: current patch is %s",
                      self.patchname)
    self.column = get_column_id(self.worksheet, self.patchname)

  def current_patch(self):
    """
    Find the patching currently in effect.
    
    If no patching is currently known, it steps through columns E through I of 
    row 2 (index 1) until it finds a non-empty cell.
    """
    self.patchname = None
    for column in range(5,10):
      self.logger.debug("current_patch: checking column %d", column)
      if self.worksheet.cell(row=1, column=column).value:
        self.logger.debug("current_patch: found %s", 
                          self.worksheet.cell(row=1, column=column).value)
        if self.patchname:
          self.logger.error("current_patch: ambiguity: %s or %s",
             self.patchname, self.worksheet.cell(row=1, column=column).value)
          raise RuntimeException("patch ambiguity")
        else:
          self.patchname = self.worksheet.cell(row=0,column=column).value
          break
      else:
        pass
    return self.patchname

  def get(self, column_name, row):
    """
    Returns value for column name in the row, including merged cells
    """
    column = get_column_id(self.worksheet, column_name)
    column_data = get_column(self.worksheet, column_name)
    while row > 0:
      if self.worksheet.cell(row=row, column=column).value:
        return self.worksheet.cell(row=row, column=column).value
      else:
        row -= 1
    return None
  
  def get_patching(self):
    """
    """
    IF_channel = {}
    for IF in range(1,17):
      rx_chan = {}
      row = get_row_number(self.worksheet, self.column, IF)
      self.logger.debug("get_patching: IF %d is in row %d", IF, row)
      for item in ["Band", "Receiver", "Pol", "IF"]:
        value= self.get(item, row)
        if value == None:
          self.logger.error("get_patching: no %s for row %d", item, row)
        else:
          rx_chan[item] = value
      IF_channel[IF] = rx_chan
    return IF_channel

  def report_patching(self):
    IF_report = {}
    for IF in self.patching.keys():
      rx_chan = self.patching[IF]
      IF_report[IF] = "R"+str(rx_chan["Receiver"]) \
                     +"-"+str(rx_chan["Band"]) \
                     +"-"+str(rx_chan["Pol"]) \
                     +"-IF"+str(label_map[str(rx_chan["IF"])])
    return IF_report

  def get_signals(self, device):
    """
    """
    try:
      inputs = get_column(self.worksheet, device)[1:]
    except TypeError:
      self.logger.error("get_signals: device %s is not known", device)
      raise RuntimeError("device %s is not known; check capitalization." % device)
    inputs = get_column(self.worksheet, device)[1:]
    self.logger.debug("get_signals: Column '%s' values: %s", device, inputs)
    sig_props = {}
    for index in range(len(inputs)):
      # row 2 is labelled 3 in the spreadsheet (for openpyxl 1.x)
      row = index+2
      channel_ID = inputs[index]
      self.logger.debug("get_signals: row %d input is %s", row, channel_ID)
      if inputs[index]:
        sig_props[channel_ID] = {}
        for item in ["Band", "Receiver", "Pol", "IF"]:
          value= self.get(item, row)
          if value == None:
            self.logger.error("get_signals: no %s for row %d", item, row)
          else:
            sig_props[channel_ID][item] = value
    return sig_props
  
  def get_inputs(self, device):
    """
    """
    try:
      inputs = get_column(self.worksheet, device)[1:]
    except TypeError:
      self.logger.error("get_inputs: device %s is not known", device)
      raise RuntimeError("device %s is not known; check capitalization." % device)
    inputs = get_column(self.worksheet, device)[1:]
    self.logger.debug("get_inputs: Column '%s' values: %s", device, inputs)
    channels = {}
    for index in range(len(inputs)):
      # row 2 is labelled 3 in the spreadsheet (for openpyxl 1.x)
      row = index+2
      channel_ID = inputs[index]
      self.logger.debug("get_inputs: row %d input is %s", row, channel_ID)
      if inputs[index]:
        channels[channel_ID] = {}
        channels[channel_ID]['RF'] = 'R'+str(self.get('Receiver',row)) \
                                    +'-'+str(self.get('Band',row)) \
                                    +str(self.get('Pol',row))
        channels[channel_ID]['IF'] = self.get('IF',row)
    return channels
  
