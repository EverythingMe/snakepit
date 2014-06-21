from __future__ import absolute_import
from .. import ServerTransport

import zmq
import threading


class Server(ServerTransport):
    def __init__(self, handler, endpoint, context=None):
        self._context = context or zmq.Context.instance()
        self._handler = handler
        self._endpoint = endpoint
        self._asked_to_stop = False

    def listen(self):
        self._thread = threading.Thread(target=self._listen)
        self._thread.start()

    def stop(self):
        self._asked_to_stop = True
        self._thread.join()

    def _listen(self):
        socket = self._context.socket(zmq.REP)
        socket.bind('tcp://{}'.format(self._endpoint))

        while not self._asked_to_stop:
            if socket.poll(timeout=1000):
                data = socket.recv_json()
                response = self.handle(data['callName'], *data['args'], **data['kwargs'])
                socket.send_json(response)
        socket.close()
