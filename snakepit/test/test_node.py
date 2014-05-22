from __future__ import absolute_import

from ..node import Node
from ..pool import Pool
from ..registry import StaticRegistry

from unittest import TestCase

__author__ = 'bergundy'


class NetworklessNode(Node):
    def listen(self):
        pass


class LocalPool(Pool):
    def call(self, peer, callName, *args, **kwargs):
        pass


class NodeTestCase(TestCase):
    def setUp(self):
        self.node = NetworklessNode('local', )

    def testDoLocalNode(self):
        pass


