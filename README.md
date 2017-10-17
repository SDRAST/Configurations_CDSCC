CDSCC
=====

Equipment configurations for the DSN Canberra DSCC. 

The apps/ directory contains some useful site specific server
and client implementations. Right now, apps/server/ contains 
two servers specfic to DSS43: `dss43k2_server.py` and 
`wbdc_server.py`. The latter is a legacy server that will be 
deprecated. The former is the DSS43 master server that accepts
and delegates commands to downstream hardware servers. Most 
commands work asynchronously. 
