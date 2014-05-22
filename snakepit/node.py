from __future__ import absolute_import

import hash_ring
from hashlib import md5

class Node(object):
    def __init__(self, endpoint, registry, pool, handler):
        self._handler = handler
        self._endpoint = endpoint
        self._pool = pool
        self._registry = registry
        self._ring = None

    def start(self):
        self.listen()
        self._registry.register(self._endpoint)
        self._ring = hash_ring.HashRing(self._registry.getNodes())
        self._registry.watch(self._onPeersChange)

    def _onPeersChange(self, endpoints):
        pass

    def _makeKey(self, callName, *args, **kwargs):
        return md5('::'.join((callName, args, tuple(sorted(kwargs.items()))))).hexdigest()

    def _getPeer(self, key):
        return self._ring.get_node(key)

    def do(self, callName, args, kwargs):
        key = self._makeKey(callName, *args, **kwargs)

        peer = self._getPeer(key)

        if peer == self._endpoint:
            return getattr(self._handler, callName)(*args, **kwargs)
        else:
            return self._pool.call(peer, callName, *args, **kwargs)

    def listen(self):
        raise NotImplementedError("PLEASE IMPLEMENT ME KTXBAI")
