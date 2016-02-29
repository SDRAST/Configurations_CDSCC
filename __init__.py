"""
Configuration information for CDSCC

dict 'ttcrx' is a structure which contains all the receivers and
polarization channels of DSN telemetry, tracking and control
receivers.

dict 'feed' contains all the feed names and positions

References
==========
http://deepspace.jpl.nasa.gov/dsndocs/810-005/302/302C.pdf
"""
import logging

logger = logging.getLogger(__name__)

cfg = {34: {},
       35: {},
       43: {},
       45: {}}

feeds = {34: {},
         35: {},
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
            'type': 'cas'}}

wrap = {43: {'stow_az': 17,
             'wrap':    {'center':135}}}

