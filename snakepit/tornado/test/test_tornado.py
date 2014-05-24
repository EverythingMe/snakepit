from __future__ import absolute_import
from ..pool import Pool
from ..server import Server
from ...group import Group
from ...registry import StaticRegistry
from ...handler import AsyncHandler
from tornado.concurrent import Future
from tornado.testing import get_unused_port, gen_test, AsyncTestCase

import logging

logging.basicConfig(level=logging.DEBUG)


class KVHandler(AsyncHandler):
    def __init__(self, name):
        self._name = name
        self._kv = {}

    def do(self, callName, *args, **kwargs):
        res = super(KVHandler, self).do(callName, *args, **kwargs)
        if not isinstance(res, Future):
            future = Future()
            future.set_result(res)
            return future
        else:
            return res

    def get(self, key):
        print "GET %s %s" % (self._name, key)
        return self._kv.get(key, None)

    def set(self, key, value):
        print "SET %s %s => %s" % (self._name, key, value)
        self._kv[key] = value


def hashKey(fn, key, *args, **kwargs):
    return key


class ZMQTestCase(AsyncTestCase):
    def setUp(self):
        super(ZMQTestCase, self).setUp()

        self.registry = StaticRegistry('Test')

        self.groups = []
        for i in xrange(3):
            port = get_unused_port()
            endpoint = '127.0.0.1:{}'.format(port)
            handler = KVHandler('{}'.format(i))
            pool = Pool(io_loop=self.io_loop)
            server = Server(endpoint, handler, io_loop=self.io_loop)
            group = Group(server, self.registry, pool, hashKey)
            group.start()
            self.groups.append(group)

    @gen_test
    def testEndToEnd(self):
        local = self.groups[0]

        for i in xrange(10):
            yield local.do('set', "k%d" % i, "val%d" % i)

        for i in xrange(10):
            res = yield local.do('get', 'k%d'%i)
            print res

        assert False

