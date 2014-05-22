from __future__ import absolute_import

import hash_ring
from hashlib import md5


class Group(object):
    def __init__(self, server, registry, pool, hashFunc = None):
        self._handler = server.handler
        self._pool = pool
        self._registry = registry
        self._ring = None
        self._hashFunc = hashFunc or self.hashKey
        self._server = server

    def start(self):
        self._server.listen()
        self._registry.register(self.localEndpoint())
        self._ring = hash_ring.HashRing(self._registry.getEnpoints())
        self._registry.watch(self._onPeersChange)

    def _onPeersChange(self, endpoints):

        self._ring = hash_ring.HashRing(endpoints)

    def hashKey(self, callName, *args, **kwargs):
        return md5(callName + '::'.join(map(str, args + tuple(sorted(kwargs.iteritems()))))).hexdigest()

    def _getPeer(self, key):
        return self._ring.get_node(key)

    def localEndpoint(self):

        return self._server.endpoint

    def do(self, callName, *args, **kwargs):


        key = self._hashFunc(callName, *args, **kwargs)

        peer = self._getPeer(key)

        print self.localEndpoint(), callName, args, kwargs, peer

        if peer == self.localEndpoint():
            return self._handler.do(callName, *args, **kwargs)
        else:
            return self._pool.getClient(peer).call(callName, *args, **kwargs)


