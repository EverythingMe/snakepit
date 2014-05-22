from __future__ import absolute_import

__author__ = 'dvirsky'
import logging; logging = logging.getLogger(__name__)

from functools import wraps, partial

from snakepit.handler import BaseHandler
from snakepit.test.test_node import LocalPool, NetworklessGroup
from snakepit.registry import StaticRegistry
import random


class LRUHandler(BaseHandler):

    _funcs = {}

    def get(self, func,  *args, **kwargs):

        f = self._funcs[func]
        return f(*args, **kwargs)


    @classmethod
    def funcName(cls, f):

        mod = f.__globals__.get('__name__')
        ret=  '%s.%s' % (mod, f.__name__)
        logging.debug("Name for func %s: %s", f, ret)
        return ret

    @classmethod
    def registerFunc(cls, func):

        cls._funcs[cls.funcName(func)] = func

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
    node = NetworklessGroup(ep, __registry, __pool, __handler)
    node.start()
    _nodes.append(node)
    __node = node


def lru_cache(fn=None, maxSize=1000):


    if fn is None:
        return partial(lru_cache, maxSize=maxSize)

    __handler.registerFunc(fn)


    @wraps(fn)
    def wrapper(*args,**kwargs):
        return __node.do('get', LRUHandler.funcName(fn), *args, **kwargs)

    return wrapper


@lru_cache
def foo(bar):

    return bar*3



if __name__ == '__main__':


    init()
    init()
    init()


    for i in xrange(20):
        print foo('foo%s'%i)



