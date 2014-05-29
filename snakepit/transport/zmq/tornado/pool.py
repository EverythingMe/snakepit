from __future__ import absolute_import

from ..pool import Pool as BasePool
from tornado.ioloop import IOLoop
from zmq.eventloop import ioloop
from .client import Client

import zmq

ioloop.install()


class Pool(BasePool):
    def __init__(self, context=None, io_loop=None):
        self._context = context or zmq.Context()
        self._io_loop = io_loop or IOLoop.current()
        self._peers = {}

    def call(self, peer, callName, *args, **kwargs):
        if peer not in self._peers:
            self._peers[peer] = Client(peer, self._context, self._io_loop)

        return self._peers[peer].call(callName, *args, **kwargs)
