"""
Configuration information for CDSCC

dict 'cfg' is a structure which contains all the receivers and
polarization channels of DSN telemetry, tracking and control
receivers.

dict 'feed' contains all the feed names and positions

References
==========
https://deepspace.jpl.nasa.gov/dsndocs/810-005/101/101F.pdf
https://deepspace.jpl.nasa.gov/dsndocs/810-005/302/302C.pdf

Notes on beam efficiency
========================
In [1]: from MonitorControl.Configurations.CDSCC import *
In [2]: from Radio_Astronomy import *

In [3]: forward_gain(0.766, pi*35**2, 300./8400)
Out[3]: 74.63040657733823
In [4]: forward_gain(0.72, pi*35**2, 300./8400)
Out[4]: 74.36144384532489

In [5]: antenna_solid_angle(0.766, pi*35**2, 300./8400)
Out[5]: 4.3268237639205843e-07
In [6]: antenna_solid_angle(0.72, pi*35**2, 300./8400)
Out[6]: 4.603259726615511e-07

The measured HPBW is 0.032 deg

In [7]: beam_solid_angle(pi*0.032/180,pi*0.032/180)
Out[7]: 3.5247890878359627e-07

In [8]: beam_efficiency(4.3268237639205843e-07,3.5247890878359627e-07)
Out[8]: 0.8146366203374346
In [9]: beam_efficiency(4.603259726615511e-07,3.5247890878359627e-07)
Out[9]: 0.7657158833458915
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

gain = {43: {"L": 61.04,
             "S": 63.59,
             "X": 74.63,
             "X-SX": 74.36}}

hpbw = {43: {"L": 0.162,
             "S": 0.118,
             "X": 0.032}}

def rel_gain43(elev):
  """
  Reference: report attached to e-mail from Shinji 05/24/2015 07:26 AM filed
  in Canberra/Calibration
  """
  a = -0.000988569
  b = 42.5931
  c =  1.78419
  return a*(elev-b)**2 + c

