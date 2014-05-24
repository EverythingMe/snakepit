from __future__ import absolute_import

from ..pool import Pool as BasePool
from tornado import ioloop, gen
from .client import Client

import logging

log = logging.getLogger(__name__)


class Pool(BasePool):
    def __init__(self, io_loop=None, connect_timeout=1.0, read_timeout=1.0):
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._io_loop = io_loop or ioloop.IOLoop.current()
        self._peers = {}

    @gen.coroutine
    def call(self, peer, callName, *args, **kwargs):
        if peer not in self._peers:
            self._peers[peer] = yield Client(peer,
                                             self._io_loop,
                                             self._connect_timeout,
                                             self._read_timeout).connect()
            log.info('Established connection to %s', peer)

        try:
            res = yield self._peers[peer].call(callName, *args, **kwargs)
        except Exception:
            del self._peers[peer]
            raise
        raise gen.Return(res)
