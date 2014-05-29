from __future__ import absolute_import
from .. import ServerTransport
from tornado.ioloop import IOLoop
from tornado.tcpserver import TCPServer
from tornado import gen, concurrent
from functools import partial

import logging
import json

log = logging.getLogger(__name__)


class SPConnection(object):
    def __init__(self, stream, handler):
        self._stream = stream
        self._handler = handler
        self._start_processing_requests()

    @gen.engine
    def _start_processing_requests(self):
        while True:
            data = yield gen.Task(self._stream.read_until, '\r\n')
            log.debug('New request: %r', data)
            try:
                msg = json.loads(data)
                key = msg['key']
                method = msg['method']
                args = msg['args']
                kwargs = msg['kwargs']
            except (KeyError, ValueError):
                log.error('Malformed response data: %s', data)
                continue
            try:
                future = self._handler(method, *args, **kwargs)
            except Exception as e:
                log.exception('Failed to handle request: %s', key)
                future = concurrent.TracebackFuture()
                future.set_exception(e)

            future.add_done_callback(partial(self._on_future_finished, key))

    def _on_future_finished(self, key, future):
        response = {'key': key}
        if future.exception():
            response['exception'] = str(future.exception())
        else:
            response['result'] = future.result()

        self._stream.write(json.dumps(response) + '\r\n')


class SPServer(TCPServer):
    def __init__(self, handler, *args, **kwargs):
        self._handler = handler
        super(SPServer, self).__init__(*args, **kwargs)

    def handle_stream(self, stream, address):
        log.info('New connection from: %s', address)
        SPConnection(stream, self._handler)


class Server(ServerTransport):
    def __init__(self, handler, endpoint=None, io_loop=None):
        self._io_loop = io_loop or IOLoop.current()
        self._server = None
        super(Server, self).__init__(handler, endpoint)

    def listen(self):
        self._server = SPServer(self.handle, io_loop=self._io_loop)
        self._socket.setblocking(0)
        self._socket.listen(128)
        self._server.add_socket(self._socket)

    def stop(self):
        self._server.stop()

    @gen.coroutine
    def handle(self, method, *args, **kwargs):
        res = super(Server, self).handle(method, *args, **kwargs)
        if isinstance(res, concurrent.Future):
            res = yield res
        raise gen.Return(res)
