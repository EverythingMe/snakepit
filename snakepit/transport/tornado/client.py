from __future__ import absolute_import
from .. import ClientTransport, RemoteException, TransportException
from tornado import concurrent, gen
from trickle import Trickle
from snakepit.group import Group

import logging
import socket
import json

log = logging.getLogger(__name__)


class Client(ClientTransport):
    def __init__(self, endpoint, io_loop, connect_timeout, read_timeout):
        host, port = endpoint.split(':')
        self._endpoint = host, int(port)
        self._io_loop = io_loop
        self._futures = dict()
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._reading = False
        self._socket = None
        self._stream = None

    @gen.coroutine
    def connect(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._stream = Trickle(self._socket, io_loop=self._io_loop)
        yield self._stream.connect(self._endpoint, self._connect_timeout)
        raise gen.Return(self)

    @gen.engine
    def _start_read_loop(self):
        while self._futures:
            #data = yield self._stream.read_until('\r\n', timeout=self._read_timeout)
            data = yield self._stream.read_until('\r\n')
            try:
                msg = json.loads(data)
                key = msg['key']
            except KeyError:
                log.error('Malformed response data: %s', data)
                continue
            try:
                future = self._futures[key]
            except KeyError:
                log.warn('No pending request for key: %s', key)
                continue

            if 'result' in msg:
                future.set_result(msg['result'])
            elif 'exception' in msg:
                future.set_exception(RemoteException(msg['exception']))
            else:
                logging.error('Response contains no `result` or `exception`: %s', msg)

            del self._futures[key]

        self._reading = False

    def _ensure_reading(self):
        if not self._reading:
            self._start_read_loop()
            self._reading = True

    def call(self, method, *args, **kwargs):
        key = Group.hash_key(method, *args, **kwargs)
        if key in self._futures:
            return self._futures[key]

        future = concurrent.TracebackFuture()

        msg = json.dumps({
            'key': key,
            'method': method,
            'args': list(args),
            'kwargs': kwargs
        }) + '\r\n'
        self._stream.write(msg)
        self._futures[key] = future
        self._ensure_reading()
        return future
