### Proposed DSS43K2 Client syntax

Dean Shaff 29/12/2017

#### Motivation/Background

Many of the DSS43K2 Server functions work asynchronously. This means that calling
a server side function will return immediately. By "calling a server-side function",
I mean creating a `Pyro4.Proxy` object corresponding the remote server, and
calling the server method. While the interface is as if we're interacting with a
local Python object, there are numerous differences and indeed _limitations_ that
prevent us from interacting with these remote methods as if they were methods
of a normal Python object. These differences have to do with the fact that some
Python objects simply aren't serializable using Pyro4's recommended serializers.
Functions, for example, are not "first class citizens" in the Pyro4 client/server
model, in the sense that we cannot pass functions as arguments to
`Pyro4.core._RemoteMethod`s:

```python
# say we have some server called "BasicServer", with some
# method "dummy", that simply accepts any argument or keyword
# argument and returns None
import Pyro4

ns = Pyro4.locateNS("localhost", 9090)
p = Pyro4.Proxy(ns.lookup("BasicServer"))

def f(x):
    return x

p.dummy()
# >>> None
p.dummy(f)
# >>>
# <ipython-input-17-b2a01f0fc042> in <module>()
# ----> 1 p.dummy(f)
#
# /path/to/python3.5/site-packages/Pyro4/core.py in __call__(self, *args, **kwargs)
#     184         for attempt in range(self.__max_retries + 1):
#     185             try:
# --> 186                 return self.__send(self.__name, args, kwargs)
#     187             except (errors.ConnectionClosedError, errors.TimeoutError):
#     188                 # only retry for recoverable network errors
#
# /path/to/python3.5/site-packages/Pyro4/core.py in _pyroInvoke(self, methodname, vargs, kwargs, flags, objectId)
#     468                         if sys.platform == "cli":
#     469                             util.fixIronPythonExceptionForPickle(data, False)
# --> 470                         raise data
#     471                     else:
#     472                         return data
#
# SerializeError: unsupported serialized class: builtins.function
```

Normally this isn't much of an issue with Pyro4. However, issues arise when things
need to work asynchronously. Client/server communication needs to be asynchronous
for a few reasons:

- Server side functions can take a long time to execute. Take boresight, for
example. Boresight takes about 5 minute to run. If we try to return results of
the boresight from this function, the client will hang for 5 minutes. This is an
issue for two reasons. One, the client is hung up for the duration of the call.
This issue is mitigated by placing the function call in a thread. The other,
more sticky issue has to do with how Pyro4 deals with remote method calls.
Pyro4 keeps the connection between client and server open for the duration of
the remote method call. This is normally not an issue, because remote methods
generally execute within very short time scales -- scales much shorter than
Pyro4's timeout. If we go in knowing that our function is going to take a longer
time to execute than Pyro4's timeout, then we are left with two options:

1. Set the timeout to infinite.
2. Have server side methods call client side functions when the server side functions
finish executing, sort of like AJAX calls.

The latter is a more robust solution, because results show up when they're ready,
almost independent of the condition of the connection between client and server.

- Results from `Pyro4.core._RemoteMethod`s can take arbitrary amounts of time to
show up. This could be an issue with the server and its subsequent connection to
downstream hardware servers or the connection between client and server.
Operating DSS43 from Abu Dhabi has alerted me to the reality of unreliable
network connections. Download/Upload speeds between Abu Dhabi and Canberra can
range from as low as 20kb/s to as high as 2mb/s. A seemingly trivial call to a
 DSS43K2Server `_RemoteMethod` can take an achingly long time. A synchronous
version of the DSS43K2Server method that gets the antenna's current Az/El could
return seemingly instantaneously or take 10 seconds. Instead of blocking
client-side functionality while it takes arbitrary amounts of time to call
`_RemoteMethod`s, why not simply call a client side function when results from
the server show up?

Pyro4 is designed to work synchronously, but we can engage a sort of asynchronous
mode. We do this by registering callbacks on a client side `Pyro4.Daemon`:

```python
# client_with_callbacks.py
import threading

import Pyro4

class ClientWithCallbacks(object):

    def __init__(self,host, port
                    server_name="BasicServer",
                    objectId="ClientWithCallbacks"):

        self.daemon = Pyro4.Daemon(host="localhost", port=9090)
        self.daemon.register(self, objectId=objectId)
        self.daemon_thread = threading.Thread(target=self.daemon.requestLoop)
        self.daemon_thread.daemon = True
        self.daemon_thread.start()
        ns = Pyro4.locateNS(host, port)
        self.server = Pyro4.Proxy(ns.lookup(server_name))

    @Pyro4.expose
    def handler1(self, results):
        print(results)

    def __getattr__(self, attr):
        return getattr(self.server, attr)

```

Of course we could inherit from the `Pyro4.Proxy` class as an alternative to
using a `__getattr__` method.

The following snippet shows how we might use the `ClientWithCallbacks` setup:

```python
# basic_server.py
class BasicServer(object):
    ...

    @Pyro4.expose
    def dummy_with_callback(self, cb_info):
        handler = cb_info["handler"]
        cb_name = cb_info["cb_name"]
        getattr(handler, cb_name)("Just called this from server!!")

# test_client.py
from client_with_callbacks import ClientWithCallbacks

client = ClientWithCallbacks("locahost", 9090, server_name="BasicServer")

client.dummy_with_callback({"handler": client, "cb_name": "handler1"})
# >>> Just called this from server!!
```

This is a rather cumbersome interface, and it doesn't lend itself well to a
command line interface. I think we can shape this into something a little
easier to stomach, however.

### Proposed (synchronous) client syntax 

```python
c = DSS43Client(*args, **kwargs)
el_offset, xel_offset = c.get_offsets()
for i in range(3):
    new_el_offset, new_xel_offset = c.boresight(el_offset, xel_offset) # this blocks for duration of run.
    el_offset, xel_offset = new_el_offset, new_xel_offset

c.minical()

```

With this syntax, we act like everything is happening synchronously, even
though results show up asynchronously. Because we don't have to worry about
timeouts, we can set up appropriate `while` loops to make sure that final
callbacks actually get called.

Many of the `DSS43K2Server` remote methods have associated with them _two_
callback functions. One is fired when the method call is complete (say, when
boresight is completed), and the other is fired when new updates show up.
For boresight, this might be every time the offset is changed. Nothing is
preventing us from creating update or completion callbacks, and passing them
as arguments to the client side version of the `DSS43K2Server` method.


```python
c = DSS43K2Client(*args, **kwargs)
el_offset, xel_offset = c.get_offsets() # no need for _this_ to be asynchronous when we're scripting

def boresight_updates_callback(updates):
    """Fired when boresight updates appear"""
    print(updates)

def boresight_callback(results):
    """Fired when boresight is finished"""
    print(results)

for i in range(3):
    new_el_offset, new_xel_offset = c.boresight(el_offset, xel_offset,
                                    update_cb=boresight_updates_callback,
                                    cb=boresight_callback)
    el_offset, xel_offset = new_el_offset, new_xel_offset

```
