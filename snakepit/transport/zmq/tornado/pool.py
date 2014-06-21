from __future__ import absolute_import
from contextlib import contextmanager
from ..pool import Pool as BasePool
from tornado.ioloop import IOLoop
from tornado.gen import coroutine, Return
from zmq.eventloop import ioloop
from .client import Client

import zmq

ioloop.install()


class Pool(BasePool):
    def __init__(self, context=None, io_loop=None):
        self._context = context or zmq.Context(io_threads=8)
        self._io_loop = io_loop or IOLoop.current()
        self._peers = {}

    @contextmanager
    def pooledConnection(self, peer):
        pool = self._peers.setdefault(peer, [])
        try:
            client = pool.pop()
        except IndexError:
            client = Client(peer, self._context, self._io_loop)
        yield client

        pool.append(client)

    @coroutine
    def call(self, peer, method, *args, **kwargs):
        with self.pooledConnection(peer) as client:
            result = yield client.call(method, *args, **kwargs)
            raise Return(result)
