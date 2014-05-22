from __future__ import absolute_import

from ..group import Group
from ..pool import Pool
from ..registry import StaticRegistry
from ..handler import BaseHandler
from ..transport import ServerTransport, ClientTransport
from unittest import TestCase

import logging
logging.basicConfig(level=0)


class MockNetwork(object):

    _sockets = {}

    @classmethod
    def listen(cls, addr, server):

        if addr in cls._sockets:
            raise RuntimeError("Port %s already taken" % addr)

        cls._sockets[addr] = server

    @classmethod
    def connect(cls, addr):

        return cls._sockets[addr]



class MockServer(ServerTransport):

    def listen(self):

        MockNetwork.listen(self._endpoint, self._handler)


class MockClient(ClientTransport):


    def __init__(self, handler):
        self._handler = handler

    def call(self, callName, *args, **kwargs):
        return self._handler.do(callName, *args, **kwargs)


class LocalPool(Pool):


    def call(self, endpoint, callName, *args, **kwargs):

        conn = MockClient(MockNetwork.connect(endpoint))
        return conn.call(callName, *args, **kwargs)


class KVHandler(BaseHandler):

    def __init__(self, name):
        self._name = name
        self._kv = {}


    def get(self, key):

        print "GET %s %s" % (self._name, key)
        return self._kv.get(key, None)

    def set(self, key, value):

        print "SET %s %s => %s" % (self._name, key, value)
        self._kv[key] = value

def hashfunc(callName, *args, **kwargs):

    return args[0]

class NodeTestCase(TestCase):


    def setUp(self):
        self.registry = StaticRegistry('Test')

        self.nodes = []
        for i in xrange(3):
            ep = "localhost:808%d" % i
            handler = KVHandler(ep)
            pool = LocalPool()
            server = MockServer(ep, handler)
            group = Group(server, self.registry, pool, hashFunc=hashfunc)

            group.start()
            self.nodes.append(group)


    def testDoLocalNode(self):

        localhost = self.nodes[0]

        for i in xrange(10):

            localhost.do('set', "k%d" % i, "val%d" % i)

        for i in xrange(10):

            print localhost.do('get', 'k%d'%i)


        self.assertTrue(False)
