from __future__ import absolute_import
from ...transport import ServerTransport
from zmq.eventloop import zmqstream
from tornado.ioloop import IOLoop
from tornado.gen import coroutine
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

    @coroutine
    def _on_recv(self, msgs):
        for msg in msgs:
            data = json.loads(msg)
            response = self._handler.do(data['callName'], *data['args'], **data['kwargs'])
            if isinstance(response, Future):
                response = yield response
            self._stream.send(json.dumps(response))
