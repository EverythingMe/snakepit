from __future__ import absolute_import
import logging;
import tornado.web

logging = logging.getLogger(__name__)
import pylru
from hashlib import md5


import time
from threading import Timer
class KVStoreHandler(object):

    def __init__(self, name, maxSize = 1000000):

        self._store = pylru.lrucache(maxSize)
        self._name = name
        self._start = time.time()
        self._numWrites = 0
        self._numReads = 0
        self._numHits = 0
        self._numMisses = 0
        self.getStats()


    def set(self, key, value):
        self._numWrites+=1
        self._store[key] = value


    def get(self, key):

        self._numReads += 1
        try:
            v = self._store[key]
            if v is not None:
                self._numHits += 1
            return v
        except Exception:
            pass

        self._numMisses += 1


    def getStats(self):

        now = time.time()
        elapsed = now - self._start
        print "Node stats: %s" % {
            'size': len(self._store),
            'reads_per_sec': self._numReads/(elapsed or 1),
            'writes_per_sec': self._numWrites/(elapsed or 1),
            'hit_rate': 100*self._numHits/float(self._numHits+self._numMisses or 1)
        }

        self.timer = Timer(2, self.getStats)
        self.timer.daemon = True
        self.timer.start()


    @classmethod
    def makeKey(cls, callName, key, *args, **kwargs):

        return md5(key).hexdigest()



class KeyValueApplication(tornado.web.Application):
    pass
if __name__ == '__main__':

    import random

    h = KVStoreHandler('foo', 1000000)

    for _ in xrange(100000000):

        rk = 'key%d' % random.randint(1,10000000)
        wk = 'key%d' % random.randint(1,10000000)

        h.set(wk, 'value______%s' % wk)
        h.get(rk)



