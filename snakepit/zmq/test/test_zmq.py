from __future__ import absolute_import
from ..pool import Pool
from ..server import Server
from ...group import Group
from ...registry import StaticRegistry
from ...handler import BaseHandler
from tornado.testing import get_unused_port
from unittest import TestCase
from hashlib import md5

import zmq


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


def hashKey(fn, key, *args, **kwargs):
    return md5(key).hexdigest()


class ZMQTestCase(TestCase):
    def setUp(self):
        super(ZMQTestCase, self).setUp()

        self.registry = StaticRegistry('Test')
        self.context = zmq.Context(1)

        self.groups = []
        for i in xrange(3):
            port = get_unused_port()
            endpoint = '127.0.0.1:{}'.format(port)
            handler = KVHandler('{}'.format(i))
            pool = Pool(context=self.context)
            server = Server(endpoint, handler, self.context)
            group = Group(server, self.registry, pool, hashKey)
            group.start()
            self.groups.append(group)

    def testEndToEnd(self):
        local = self.groups[0]

        for i in xrange(10):
            print local.do('set', "k%d" % i, "val%d" % i)

        for i in xrange(10):
            print local.do('get', 'k%d'%i)

        assert False
