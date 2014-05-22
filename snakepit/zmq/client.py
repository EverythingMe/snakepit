from __future__ import absolute_import
from ..transport import ClientTransport

import json
import zmq


class Client(ClientTransport):
    def __init__(self, endpoint, context):
        self._context = context
        self._endpoint = endpoint
        self._connect()

    def _connect(self):
        self._socket = self._context.socket(zmq.REQ)
        self._socket.connect('tcp://{}'.format(self._endpoint))

    def call(self, callName, *args, **kwargs):
        msg = json.dumps({
            'callName': callName,
            'args': list(args),
            'kwargs': kwargs
        })
        self._socket.send(msg)
        msg = self._socket.recv()
        return json.loads(msg)
