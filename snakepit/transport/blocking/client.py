from __future__ import absolute_import
from .. import ClientTransport, TransportException, RemoteException
from snakepit.group import Group

import logging
import socket
import json


__author__ = 'bergundy'
log = logging.getLogger(__name__)


class Client(ClientTransport):
    def __init__(self, endpoint, timeout=0.5, recv_chunk_size=4096):
        self._socket = None
        self._recv_chunk_size = recv_chunk_size
        self._timeout = timeout
        self._buffer = []
        host, port = endpoint.split(':')
        self._endpoint = host, int(port)

    def connect(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._socket.settimeout(self._timeout)
        self._socket.connect(self._endpoint)

    def call(self, method, *args, **kwargs):
        key = Group.hash_key(method, *args, **kwargs)
        msg = json.dumps({
            'key': key,
            'method': method,
            'args': args,
            'kwargs': kwargs
        }) + '\r\n'
        self._socket.send(msg)
        return self.recv_data()

    def recv_data(self):
        while True:
            data = self._socket.recv(self._recv_chunk_size)
            if len(data) == 0:
                raise TransportException('Disconnected')
            while data:
                pos = data.find('\r\n')
                if pos > -1:
                    part = data[:pos]
                    data = data[pos+2:]
                    self._buffer.append(part)
                    serialized = ''.join(self._buffer)
                    self._buffer = []
                    try:
                        msg = json.loads(serialized)
                    except ValueError:
                        raise TransportException('Invalid json response: {!r}'.format(serialized))
                    if 'result' in msg:
                        return msg['result']
                    elif 'exception' in msg:
                        raise RemoteException(msg['exception'])
                    else:
                        raise TransportException('Invalid response format: {!r}'.format(serialized))
                self._buffer.append(data)
