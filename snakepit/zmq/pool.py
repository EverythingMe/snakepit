from __future__ import absolute_import

from ..pool import Pool as BasePool
from .client import Client

import zmq


class Pool(BasePool):
    def __init__(self, context=None):
        self._context = context or zmq.Context()
        self._peers = {}

    def call(self, peer, callName, *args, **kwargs):
        if peer not in self._peers:
            self._peers[peer] = Client(peer, self._context)

        return self._peers[peer].call(callName, *args, **kwargs)

