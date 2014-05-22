from __future__ import absolute_import
from ..transport import ServerTransport

import json
import zmq
import threading


class Server(ServerTransport):
    def __init__(self, endpoint, handler, context):
        self._context = context
        super(Server, self).__init__(endpoint, handler)

    def listen(self):
        self._thread = threading.Thread(target=self._listen)
        self._thread.start()

    def _listen(self):
        socket = self._context.socket(zmq.REP)
        socket.bind('tcp://{}'.format(self._endpoint))

        while True:
            msg = socket.recv()
            data = json.loads(msg)
            response = self._handler.do(data['callName'], *data['args'], **data['kwargs'])
            socket.send(json.dumps(response))
