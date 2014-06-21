from __future__ import absolute_import

from ..group import Group
from ..pool import Pool
from ..registry import StaticRegistry
from ..transport import ServerTransport, ClientTransport
from unittest import TestCase
from snakepit.examples.kv_handler import SimpleKVHandler


class MockNetwork(object):
    _sockets = {}

    @classmethod
    def listen(cls, addr, server):
        if addr in cls._sockets:
            raise RuntimeError('Port %s already taken' % addr)

        cls._sockets[addr] = server

    @classmethod
    def connect(cls, addr):
        return cls._sockets[addr]


class MockServer(ServerTransport):
    def listen(self):
        MockNetwork.listen(self._endpoint, self)

    def stop(self):
        pass


class MockClient(ClientTransport):
    def __init__(self, server):
        self._server = server

    def call(self, method, *args, **kwargs):
        return self._server.handle(method, *args, **kwargs)


class LocalPool(Pool):
    def call(self, endpoint, method, *args, **kwargs):
        conn = MockClient(MockNetwork.connect(endpoint))
        return conn.call(method, *args, **kwargs)


class NodeTestCase(TestCase):
    def setUp(self):
        self.registry = StaticRegistry('Test')

        self.groups = []
        for i in xrange(3):
            ep = 'localhost:808%d' % i
            handler = SimpleKVHandler(ep)
            pool = LocalPool()
            server = MockServer(handler, ep)
            group = Group(server, self.registry, pool, hash_func=SimpleKVHandler.hash_key)

            group.start()
            self.groups.append(group)

    def test_group_do(self):
        group = self.groups[0]

        for i in xrange(10):
            group.do('set', 'k%d' % i, 'val%d' % i)

        for i in xrange(10):
            assert group.do('get', 'k%d' % i) == 'val%d' % i
