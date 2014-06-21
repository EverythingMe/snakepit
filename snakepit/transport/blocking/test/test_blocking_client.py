from __future__ import absolute_import
from ..pool import Pool
from ..server import Server
from snakepit.group import Group
from snakepit.registry import StaticRegistry
from snakepit.examples.kv_handler import SimpleKVHandler
from unittest import TestCase

import logging

logging.basicConfig(level=logging.DEBUG)


class TornadoTransportTestCase(TestCase):
    def setUp(self):
        super(TornadoTransportTestCase, self).setUp()

        self.registry = StaticRegistry('{} Registry'.format(self.__class__.__name__))
        self.groups = []

        for i in xrange(3):
            handler = SimpleKVHandler('{}'.format(i))
            pool = Pool()
            server = Server(handler)
            group = Group(server, self.registry, pool, SimpleKVHandler.hash_key)
            group.start()
            self.groups.append(group)

        self.group = self.groups[0]

    def tearDown(self):
        for group in self.groups:
            group.stop()

    def test_set_get(self):
        for i in xrange(10):
            self.group.do('set', 'k%d' % i, 'val%d' % i)

        for i in xrange(10):
            res = self.group.do('get', 'k%d' % i)
            self.assertEqual(res, 'val%d' % i)
