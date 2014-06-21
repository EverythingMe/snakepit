from __future__ import absolute_import, division
from .. import ServerTransport
from snakepit.transport.tornado.server import Server as BaseServer
from tornado.ioloop import IOLoop

import threading


class Server(BaseServer):
    """
    Thread wrapper over `tornado.transport.server.Server`
    """
    def __init__(self, handler):
        self.io_loop = IOLoop()
        super(Server, self).__init__(handler, io_loop=self.io_loop)
        self._serving_thread = None

    def listen(self):
        self._serving_thread = threading.Thread(target=self._listen)
        self._serving_thread.start()

    def _listen(self):
        super(Server, self).listen()
        self.io_loop.start()

    def stop(self):
        self.io_loop.add_callback(super(Server, self).stop)
        self.io_loop.add_callback(self.io_loop.stop)

    def handle(self, method, *args, **kwargs):
        return ServerTransport.handle(self, method, *args, **kwargs)
