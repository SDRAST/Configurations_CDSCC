"""
Configuration information for CDSCC

dict 'cfg' is a structure which contains all the receivers and
polarization channels of DSN telemetry, tracking and control
receivers.

dict 'feed' contains all the feed names and positions

References
==========
http://deepspace.jpl.nasa.gov/dsndocs/810-005/302/302C.pdf
"""
import logging

logger = logging.getLogger(__name__)

cfg = {43: {'S' :{'R':0,'L':0},
            'X' :{'R':0,'L':0}},
       45: {'S' :{'R':0},
            'X' :{'R':0}},
       34: {'S' :{'R':0},
            'X' :{'R':0},
            'Ka':{'R':0}},
       35: {'X' :{'R':0},
            'Ka':{'R':0}},
       36: {'S' :{'R':0},
            'X' :{'R':0},
            'Ka':{'R':0}}}

feeds = {34: {},
         35: {},
         36: {},
         43: {'P1': 0,
              'Ku': 9.2,
              'K1': 24,        # 3-ch K-band
              'P1L': 24,
              'X': 120,
              'P2': 120,
              'SX': 240,
              'P3': 240,
              'L': 300,
              'P4': 300,
              'K2A': 332.971,  # 4-ch K-band anti-clockwise feed
              'F1':  332.971,
              'P1R': 336,
              'K2C': 339.029,  # 4-ch K-band clockwise feed
              'F2':  339.029},
         45: {}}

mech = {43:{'diam': 70,
            'type': 'cas'},
        45:{'diam': 34,
            'type': 'HEF'},
        34:{'diam': 34,
            'type': 'BWG'},
        35:{'diam': 34,
            'type': 'BWG'},
        36:{'diam': 34,
            'type': 'BWG'}}

wrap = {43: {'stow_az': 17,
             'wrap':    {'center': 135}},
        45: {'stow_az': 0,
            'wrap':     {'center':  45}},
        34: {'stow_az': 0,
             'wrap':    {'center':  45}},
        35: {'stow_az': 0,
             'wrap':    {'center':  45}},
        36: {'stow_az': 0,
             'wrap':    {'center':  45}}}



def gain43(elev):
  """
  Reference: report attached to e-mail from Shinji 05/24/2015 07:26 AM filed
  in Canberra/Calibration
  """
  a = -0.000988569
  b = 42.5931
  c =  1.78419
  return a*(elev-b)**2 + c

