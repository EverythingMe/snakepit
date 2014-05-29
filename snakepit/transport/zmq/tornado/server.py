from __future__ import absolute_import
from ... import ServerTransport
from zmq.eventloop import zmqstream
from tornado.ioloop import IOLoop
from tornado.gen import coroutine, Return
from tornado.concurrent import Future

import json
import zmq


class Server(ServerTransport):
    def __init__(self, endpoint, handler, context, io_loop=None):
        self._context = context
        self._io_loop = io_loop or IOLoop.current()
        super(Server, self).__init__(endpoint, handler)

    def listen(self):
        self._socket = self._context.socket(zmq.REP)
        self._socket.bind('tcp://{}'.format(self._endpoint))
        self._stream = zmqstream.ZMQStream(self._socket, self._io_loop)
        self._stream.on_recv(self._on_recv)

    def stop(self):
        self._stream.stop_on_recv()
        self._stream.close()

    @coroutine
    def handle(self, callName, *args, **kwargs):
        res = super(Server, self).handle(callName, *args, **kwargs)
        if isinstance(res, Future):
            res = yield res
        raise Return(res)

    @coroutine
    def _on_recv(self, msgs):
        for msg in msgs:
            data = json.loads(msg)
            response = yield self.handle(data['callName'], *data['args'], **data['kwargs'])
            self._stream.send_json(response)
