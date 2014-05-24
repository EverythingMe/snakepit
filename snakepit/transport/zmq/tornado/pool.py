from __future__ import absolute_import
from contextlib import contextmanager

from ..pool import Pool as BasePool
from tornado.ioloop import IOLoop
from zmq.eventloop import ioloop
from .client import Client
import zmq
from threading import Lock
import random
ioloop.install()



class Pool(BasePool):
    def __init__(self, context=None, io_loop=None):
        self._context = context or zmq.Context(io_threads=8)
        self._io_loop = io_loop or IOLoop.current()
        self._peers = {}
        self._lock = Lock()

    @contextmanager
    def pooledConnection(self, peer):

        client = None
        with self._lock:
            pool = self._peers.setdefault(peer, [])
            if pool:
                client = pool.pop()
        if client is None:
            client =  Client(peer, self._context, self._io_loop)
        yield client

        with self._lock:
            pool.append(client)



    def call(self, peer, callName, *args, **kwargs):

        #return Client(peer, self._context, self._io_loop).call(callName, *args, **kwargs)
        if peer not in self._peers:
            self._peers[peer] = Client(peer, self._context, self._io_loop)

        return self._peers[peer].call(callName, *args, **kwargs)
