import logging
import time
import threading
import Queue
import inspect

import Pyro4

from support.pyro import Pyro4Client, config

from ..server.dss43k2_server import DSS43K2Server

__all__ = ["DSS43K2Client"]

module_logger = logging.getLogger(__name__)

class populate_client(object):
    """
    Decorator to automatically create client methods and callbacks
    corresponding to server methods.
    """
    def __init__(self, server_cls=DSS43K2Server):
        self.server_cls = DSS43K2Server

    def __call__(self, cls):

        def callback_factory(cb_name):
            """
            Factory functions for callbacks.
            """
            def callback(self, updates_or_results):
                callback.ready = True
                callback.data = updates_or_results
                if callback.then is not None:
                    callback.then(updates_or_results)

            callback.ready = False
            callback.data = None
            callback.then = None

            return callback

        def callback_updates_factory(cb_updates_name, queue):
            def updates_callback(self, updates):
                queue.put(updates)
            return updates_callback


        def method_factory(method_name, cb_name, cb_updates_name=None, updates_queue_name=None):
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
                cb_then = kwargs.pop("then", None)
                # self.logger.debug("{}: then: {}".format(method_name, cb_then))
                # self.logger.debug("{}: Called.".format(method_name))
                getattr(self.server, method_name)(*args, **kwargs)
                # now wait for callback
                cb = getattr(self, cb_name)
                with self.lock:
                    cb_ready = cb.ready
                    cb.__dict__["then"] = cb_then
                self.logger.debug("ready status for callback {}: {}".format(cb_name, cb_ready))
                self.logger.debug("callback then: {}".format(cb.then))
                while not cb_ready:
                    with self.lock:
                        cb_ready = getattr(self, cb_name).ready
                    time.sleep(0.01)
                with self.lock:
                    data = getattr(self, cb_name).data

                if updates_queue_name is None:
                    return data
                else:
                    return data, getattr(self, updates_queue_name)

            return method

        for method_name in dir(self.server_cls):
            method = getattr(self.server_cls, method_name)
            try:
                if method._async_method:

                    # print("Creating synchronous analog for method {}".format(method_name))
                    callback_name = "{}_cb".format(method_name)
                    callback = config.expose(callback_factory(callback_name))

                    updates_queue_name = "{}_updates_queue".format(method_name)
                    updates_queue = Queue.Queue()
                    callback_updates_name = "{}_cb_updates".format(method_name)
                    callback_updates = config.expose(callback_updates_factory(callback_updates_name, updates_queue))

                    client_method = method_factory(method_name, callback_name, callback_updates_name, updates_queue_name)

                    # callback = config.expose(callback(callback_name))

                    setattr(cls, callback_name, callback)
                    setattr(cls, updates_queue_name, updates_queue)
                    setattr(cls, callback_updates_name, callback_updates)
                    setattr(cls, method_name, client_method)
                else:
                    pass
            except AttributeError:
                pass
        return cls

@populate_client(server_cls=DSS43K2Server)
class DSS43K2Client(Pyro4Client):

    def __init__(self, tunnel, proxy_name,
            host="localhost", port=50001,
            objectId="DSS43K2Client", daemon=None,
            use_autoconnect=False, logger=None):

        Pyro4Client.__init__(self, tunnel, proxy_name,
            use_autoconnect=use_autoconnect, logger=logger)
        self.daemon = None
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

        if self.daemon is None:
            err_msg = "Couldn't create Daemon"
            self.logger.error(err_msg)
            raise RuntimeError(err_msg)
        self.daemon_thread = threading.Thread(target=self.daemon.requestLoop)
        self.daemon_thread.daemon = True
        self.daemon_thread.start()

        uri = self.daemon.register(self, objectId=objectId)
        self.logger.info("DSS43K2Client registered with uri {}".format(uri))
        self.tunnel.register_remote_daemon(self.daemon)
        # self.server.set_callback_handler(uri)
        self.handler_host = host
        self.handler_port = port
        self.handler_objectId = objectId
        self.handler_uri = objectId
        self.lock = threading.Lock()
