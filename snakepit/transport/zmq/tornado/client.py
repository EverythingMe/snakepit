from __future__ import absolute_import
from ... import ClientTransport
from tornado.concurrent import TracebackFuture
from zmq.eventloop import zmqstream
from collections import deque

import json
import zmq


class Client(ClientTransport):
    def __init__(self, endpoint, context, io_loop):
        self._context = context
        self._endpoint = endpoint
        self._io_loop = io_loop
        self._connect()
        self._futures = deque()

    def _connect(self):
        self._socket = self._context.socket(zmq.REQ)
        self._socket.connect('tcp://{}'.format(self._endpoint))
        self._stream = zmqstream.ZMQStream(self._socket, self._io_loop)
        self._stream.on_recv(self._on_recv)

    def _on_recv(self, msgs):
        for msg in msgs:
            future = self._futures.pop()
            res = json.loads(msg)
            future.set_result(res)

    def call(self, method, *args, **kwargs):
        future = TracebackFuture()
        msg = json.dumps({
            'callName': method,
            'args': list(args),
            'kwargs': kwargs
        })
        self._futures.appendleft(future)
        self._stream.send(msg)
        return future
