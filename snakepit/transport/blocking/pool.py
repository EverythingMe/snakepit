from __future__ import absolute_import

from snakepit.pool import Pool as BasePool
from .client import Client
from .. import RemoteException
from threading import Lock

import logging

log = logging.getLogger(__name__)


class Pool(BasePool):
    def __init__(self):
        self._peers = {}
        self._lock = Lock()

    def call(self, peer, method, *args, **kwargs):
        client = None

        with self._lock:
            pool = self._peers.setdefault(peer, [])
            if len(pool) > 0:
                client = pool.pop()

        if not client:
            client = Client(peer)
            client.connect()

        try:
            ret = client.call(method, *args, **kwargs)
        except RemoteException:
            pool.append(client)
            raise
        pool.append(client)
        return ret


