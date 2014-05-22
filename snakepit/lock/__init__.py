import random
import time
from snakepit.registry import StaticRegistry
from snakepit.test.test_node import MockServer, LocalPool

__author__ = 'dvirsky'

import logging; logging.basicConfig(level=0); logging = logging.getLogger(__name__)
import uuid
from threading import Lock, Event, Timer
from contextlib import contextmanager
from hashlib import md5

from snakepit.handler import BaseHandler
from snakepit.group import Group

DEFAULT_TTL = 10.0
DEFAULT_TIMEOUT = 1.0
class DistLock(object):

    def __init__(self, id, ttl=DEFAULT_TTL):
        self._id = id
        self._ttl = ttl
        self._lock = Lock()
        self._event = Event()
        self._timer = None

        self._acquire()
            
    def _acquire(self):

        #the token is used to verify that the releaser was the locker
        self._token=  uuid.uuid4().hex
        self._event.clear()

        if self._timer is not None:
            self._timer.cancel()
        #reset the expiration timer
        self._timer = Timer(self._ttl, self._release)
        self._timer.start()

    def acquire(self, timeout=DEFAULT_TIMEOUT, ttl=None):

        with self._lock:

            #This lock has timed out but still in the table
            if self._token is None:

                logging.info("Orphan lock! we can acquire")
                self._ttl = ttl
                self._acquire()
                return True

        start = time.time()
        #Else - try to wait unti we can
        logging.debug("Waiting for lock %s secs", timeout)
        if self._event.wait(timeout):
            waited = time.time() - start
            with self._lock:
                #someone else has acquired it...
                if self._token != None:
                    timeout -= waited
                    print timeout, waited
                    if timeout < 0.01: #we pad because this is over network dude!
                        return False
                else:

                    logging.info("Acquiring %s", self._id)
                    self._acquire()
                    return True
        else:
            waited = time.time() - start
        timeout -= waited
        print timeout, waited
        if timeout < 0.01: #we pad because this is over network dude!
            return False
        return self.acquire(timeout,ttl)


    def release(self, token):

        logging.info("Explicit release of lock %s with token %s", self._id, token)
        if token == self._token:
            self._release()
            return True

        logging.error("Trying to release a lock in someone is not the owner of!")
        return False



    def _release(self):
        logging.info("Releasing lock %s", self._id)
        with self._lock:
            self._event.set()
            self._token = None
            if self._timer:
                self._timer.cancel()

class LockHandler(BaseHandler):

    def __init__(self):

        self._lock = Lock()
        self._lockTable = {}

    def acquire(self, key, timeout=1.0, ttl=10.0):

        lock = None
        with self._lock:
            lock = self._lockTable.get(key)

            if lock is None:
                self._lockTable[key] = lock = DistLock(key, ttl)
                logging.info("Created and acquired a new lock for %s", key)
                return lock._token

        if lock is None:
            return False

        if lock.acquire(timeout, ttl):

            return lock._token

        else:
            logging.error("Could not acquire lock!")


        return False

    def release(self, key, token):


        with self._lock:

            lock = self._lockTable.get(key)
            if lock is None:
                logging.warning("Trying to release a nonexistan lock %s", key)
                return False


            if lock.release(token):
                logging.info("Released lock %s", key)
                del self._lockTable[key]
                return True

            logging.info("Could not release lock %s with token %s", key, token)
            return False

    @classmethod
    def hashKey(cls, callName, *args, **kwargs):

        return md5(callName).hexdigest()


__group = None

_nodes = []
def init(registry, pool):
    global __group
    global _nodes
    port = random.randint(1,1000000)
    ep = "localhost:%d" % port

    print ep
    server = MockServer(ep, LockHandler())
    group = Group(server,registry, pool)
    group.start()
    _nodes.append(group)
    __group = group

class LockingError(Exception):
    pass

@contextmanager
def distributed_lock(key, timeout=DEFAULT_TIMEOUT, ttl=DEFAULT_TTL):

    global __group
    if __group is None:
        raise RuntimeError("Locking group has not been initialized")

    token = __group.do('acquire', key, timeout=DEFAULT_TIMEOUT, ttl=DEFAULT_TTL)
    if not token:
        raise LockingError("Could not lock!")

    logging.info("Acquired lock!  let's execute your code mate")
    yield

    logging.info("Execution finished, releasing lock %s with token %s", key, token)

    rc = __group.do('release', key, token)
    if not rc:
        logging.warning("Could not release remote lock %s", key)




if __name__ == '__main__':


    registry = StaticRegistry('Test')
    pool = LocalPool()

    init(registry, pool)
    from threading import Thread, currentThread

    # h = LockHandler()
    # key = 'foo'
    # token = h.acquire(key,ttl=2.0)
    # print token
    # time.sleep(2.1)
    # print "'%s'" % h.release(key, token)

    def foo():
        with distributed_lock('bar',timeout=1):

            print "i'm %s!" % currentThread()
            time.sleep(3)


    t = Thread(target=foo)
    t.start()
    time.sleep(0.1)
    foo()