from __future__ import absolute_import
import logging
import hash_ring

logging = logging.getLogger(__name__)


class Group(object):
    """
    The Group is the top level object that manages everything, and is called both locally and
    """
    def __init__(self, server, registry, pool, hash_func=None):
        self._pool = pool
        self._registry = registry
        self._ring = None
        self._hash_func = hash_func or self.hash_key
        self._server = server

    def start(self):
        self._registry.watch(self._on_peers_change)
        self._server.listen()
        self._registry.register(self.local_endpoint())
        self._ring = hash_ring.HashRing(self._registry.get_endpoints())

    def stop(self):
        self._server.stop()
        self._registry.stop()

    def _on_peers_change(self, endpoints):
        logging.info("Group %s peers changed: %s", self.local_endpoint(), endpoints)
        self._ring = hash_ring.HashRing(self._registry.get_endpoints())

    @staticmethod
    def hash_key(method, *args, **kwargs):
        return method + '::'.join(map(str, args + tuple(sorted(kwargs.iteritems()))))

    def _get_peer(self, key):
        return self._ring.get_node(key)

    def local_endpoint(self):
        return self._server.endpoint

    def do(self, method, *args, **kwargs):
        key = self._hash_func(method, *args, **kwargs)

        peer = self._get_peer(key)

        if peer == self.local_endpoint():
            return self._server.handle(method, *args, **kwargs)
        else:
            return self._pool.call(peer, method, *args, **kwargs)

