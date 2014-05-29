from __future__ import absolute_import
from ..pool import Pool
from ..server import Server
from snakepit.examples.kv_handler import SimpleKVHandler
from snakepit.group import Group
from snakepit.registry import StaticRegistry
from tornado.testing import get_unused_port
from unittest import TestCase

import zmq


class ZMQTestCase(TestCase):
    def setUp(self):
        super(ZMQTestCase, self).setUp()

        self.registry = StaticRegistry('Test')
        self.context = zmq.Context(1)

        self.groups = []
        for i in xrange(3):
            port = get_unused_port()
            endpoint = '127.0.0.1:{}'.format(port)
            handler = SimpleKVHandler('{}'.format(i))
            pool = Pool(context=self.context)
            server = Server(handler, endpoint, context=self.context)
            group = Group(server, self.registry, pool, SimpleKVHandler.hash_key)
            group.start()
            self.groups.append(group)

        self.group = self.groups[0]

    def tearDown(self):
        for group in self.groups:
            group.stop()

    def testEndToEnd(self):
        for i in xrange(10):
            self.group.do('set', 'k%d' % i, 'val%d' % i)

        for i in xrange(10):
            res = self.group.do('get', 'k%d' % i)
            self.assertEqual(res, 'val%d' % i)
