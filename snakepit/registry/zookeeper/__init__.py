from __future__ import absolute_import
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import NodeExistsError, NoNodeError

from .. import Registry
from . import config

import logging

__author__ = 'nati'
logging = logging.getLogger(__name__)


class ZKRegistry(Registry):
    def __init__(self, name, hosts):
        """
        :param name: Registry name
        :param hosts: Comma-separated list of hosts to connect to
        """
        super(ZKRegistry, self).__init__(name)
        self._watcher = None
        self._user_watcher = None
        self._hosts = hosts
        self._registry_path = self._make_path()
        self._client = None
        self._connect()

    def _make_path(self, endpoint=None):
        parts = [config.base_path, self._name]
        if endpoint:
            parts.append(endpoint)
        logging.debug('/'.join(parts))
        return '/'.join(parts)

    def _connect(self):
        """
        Start the ZooKeeper client, and ensure path existence
        """
        self._client = KazooClient(hosts=self._hosts, timeout=0.5)
        self._client.start()  # TODO: handle exceptions

        self._client.add_listener(self._state_listener)
        self._client.ensure_path(self._registry_path)  # TODO: handle exceptions
        self._watcher = self._client.ChildrenWatch(self._registry_path, func=self._children_watch)
        self._children_watch(self._client.get_children(self._registry_path))

    def stop(self):
        self._client.stop()

    def _state_listener(self, state):
        """
        Handle cases where the connection to ZooKeeper disconnects, and the children update
        :param state: the current KazooState
        """
        if state == KazooState.CONNECTED:
            new_children = set(self._client.get_children(self._registry_path))
            if self._endpoints != new_children:
                self._children_watch(new_children)

    def _children_watch(self, children):
        """
        Update _endpoints on changes, and call the user's callback function
        """
        self._endpoints = set(children)
        if self._user_watcher:
            self._user_watcher(self.get_endpoints())

    def register(self, endpoint):
        """
        Add an ephemeral zookeeper node for this endpoint
        If another node is already registered under endpoint, try to delete it
        :param endpoint: our own endpoint that is how other nodes will know us, in a "host:port" format
        """
        path = self._make_path(endpoint)
        if isinstance(self._client, KazooClient):
            try:
                self._client.delete(path=path)
            except NoNodeError:
                pass
            try:
                self._client.create(path=path, ephemeral=True)
            except NodeExistsError:
                logging.exception('Could not register node')
                # node already exists, nothing to do (?)
            else:
                logging.info('Registered successfully')
            # TODO: handle more exceptions
        else:
            pass  # TODO: handle missing client

    def watch(self, callback):
        """
        Start watching the registry for changes, with a watch callback
        callback will be called on another thread so make sure it's thread safe
        :param callback: a function in the form of f([endpoint, endpoint, ...])
        """
        if callable(callback):
            self._user_watcher = callback
        else:
            raise ValueError('callback should be callable')