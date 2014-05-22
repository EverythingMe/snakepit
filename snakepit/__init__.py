from __future__ import absolute_import
from hashlib import md5

import hash_ring


__author__ = 'dvirsky'


class Pool(object):
    def call(self, peer, callName, *args, **kwargs):
        pass


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


class Registry(object):
    def __init__(self, name):
        self._name = name
        self._endpoints = set()

    def getEnpoints(self):
        """
        Return all the endpoints in the registry
        :return: a list of endpoint strings
        """
        return tuple(sorted(self._endpoints))

    def register(self, endpoint):
        """
        Register ourselves as a node in the registry
        :param endpoint: our own endpoint that is how other nodes will know us, in a "host:port" format
        :return:
        """

        raise NotImplementedError()

    def watch(self, callback):
        """
        Start watching the registry for changes, with a watch callback
        :param callback: a function in the form of f([endpoint, endpoint, ...])
        """
        raise NotImplementedError()


class StaticRegistry(Registry):
    def register(self, endpoint):
        self._endpoints.add(endpoint)
