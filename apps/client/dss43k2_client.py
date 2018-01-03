import threading

import Pyro4

from support.pyro import Pyro4Client, config

from ..server import DSS43K2Server

__all__ = ["DSS43K2Client"]

class populate_client(object):
    """
    Decorator to automatically create client methods and callbacks
    corresponding to server methods.
    """
    def __init__(self, server_cls=DSS43K2Server):
        self.server_cls = DSS43K2Server

    def __call__(self, cls):
        def callback_factory(callback_name):
            """
            Factory functions for callbacks.
            """
            def callback(self, updates_or_results):
                getattr(self.callback_name).ready = True
                getattr(self.callback_name).data = updates_or_results
            callback.ready = False
            callback.data = None
            return callback

        def method_factory(name, cb_name, cb_updates_name=None):
            """
            Factory function for client methods. Registered callbacks will get
            passed as arguments to the appropriate
            """
            def method(self, *args, **kwargs):
                kwargs["cb_info"] = {
                    "cb_handler":self,
                    "cb":cb_name,
                    "cb_updates": cb_updates_name
                }
                getattr(self.server)(*args, **kwargs)

                # now wait for callback
                with self.lock:
                    cb_ready = getattr(self, cb_name).ready
                while not cb_ready:
                    with self.lock:
                        cb_ready = getattr(self, cb_name).ready
                    time.sleep(0.01)
                with self.lock:
                    data = getattr(self, cb_name).data
                return data

            return method


        for method_name in dir(self.server_cls):
            method = getattr(self.server_cls, method_name)
            if isinstance(method, Pyro4.core._RemoteMethod) and method._async_method:
                callback_name = "{}_cb".format(method_name)
                callback = callback_factory(callback_name)
                setattr(cls, callback_name, config.expose(callback))

                client_method = method_factory(method_name)
                setattr(cls, callback_name, client_method)

        return cls

@populate_client(server_cls=DSS43K2Server)
class DSS43K2Client(Pyro4Client):

    def __init__(self, tunnel, proxy_name,
            host="localhost", port=50001,
            objectId="DSS43K2Client", daemon=None,
            use_autoconnect=False, logger=None):

        Pyro4Client.__init__(self, tunnel, proxy_name,
            use_autoconnect=use_autoconnect, logger=logger)

        if daemon:
            self.daemon = daemon
            host, port = self.daemon.locationStr.split(":")
        else:
            register_attempts = 0
            while (register_attempts < 10):
                try:
                    self.daemon = Pyro4.Daemon(host=host, port=port)
                    break
                except OSError:
                    self.logger.debug("Port {} already in use. Attempting to register callback handler on a higher port number.".format(port))
                    port += 1
                register_attempts += 1
        uri = self.daemon.register(self, objectId=objectId)
        self.logger.info("DSS43K2Client registered with uri {}".format(uri))
        self.tunnel.register_remote_daemon(self.daemon)
        self.handler_host = host
        self.handler_port = port
        self.handler_objectId = objectId
        self.handler_uri = objectId
        self.lock = threading.Lock()
