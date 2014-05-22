from __future__ import absolute_import
from snakepit.group import Group

__author__ = 'dvirsky'
import logging; logging = logging.getLogger(__name__)

from functools import wraps, partial
import backports.functools_lru_cache as local_lru

from snakepit.handler import BaseHandler
from snakepit.test.test_node import LocalPool, MockServer
from snakepit.registry import StaticRegistry
import random


class LRUHandler(BaseHandler):

    _funcs = {}

    @local_lru.lru_cache()
    def get(self, func,  *args, **kwargs):
        print "COLD CACHE MISS"
        f = self._funcs[func]
        return f(*args, **kwargs)


    @classmethod
    def funcName(cls, f):

        mod = f.__globals__.get('__name__')
        ret=  '%s.%s' % (mod, f.__name__)
        logging.debug("Name for func %s: %s", f, ret)
        return ret

    @classmethod
    def registerFunc(cls, func, maxSize):

        cls._funcs[cls.funcName(func)] = local_lru.lru_cache(maxsize=maxSize)(func)

__handler = LRUHandler()
__node = None
__registry = StaticRegistry('Test')
__pool = LocalPool()

_nodes = []
def init():
    global __node
    global _nodes
    port = random.randint(1,1000000)
    ep = "localhost:%d" % port
    print ep
    server = MockServer(ep, __handler)
    group = Group(server,__registry,__pool)
    group.start()
    _nodes.append(group)
    __node = group


def lru_cache(fn=None, maxSize=1000):

    if fn is None:
        return partial(lru_cache, maxSize=maxSize)

    __handler.registerFunc(fn,maxSize)


    @local_lru.lru_cache(max(maxSize/8, 1))
    def wrapper(*args,**kwargs):
        return __node.do('get', LRUHandler.funcName(fn), *args, **kwargs)

    return wrapper



@lru_cache(maxSize=10)
def foo(bar):

    print "HOTCACHE MISS"
    return bar*3



if __name__ == '__main__':


    init()
    init()
    init()


    for n in xrange(1000):

            print foo('foo%s'%random.randint(1,100))



