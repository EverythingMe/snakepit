from __future__ import absolute_import

from ..node import Node
from ..pool import Pool
from ..registry import StaticRegistry

from collections import defaultdict
from unittest import TestCase


class NetworklessNode(Node):
    def listen(self):
        pass


class LocalPool(Pool):
    def __init__(self):
        self._peers = defaultdict(dict)

    def call(self, peer, callName, *args, **kwargs):
        getattr(self._peers[peer], callName)(*args, **kwargs)


class NodeTestCase(TestCase):
    def setUp(self):
        self.endpoint = 'local'
        self.registry = StaticRegistry('Test')
        self.pool = LocalPool()
        self.handler = dict()
        self.node = NetworklessNode(self.endpoint, self.registry, self.pool, self.handler)

    def testDoLocalNode(self):
        pass



