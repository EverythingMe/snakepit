import logging

from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import NodeExistsError

from snakepit.registry import Registry
from snakepit.zookeeper import config

__author__ = 'nati'


class ZKRegistry(Registry):
    def __init__(self, name, hosts):
        """
        :param name: Registry name
        :param hosts: Comma-separated list of hosts to connect to
        """
        super(ZKRegistry, self).__init__(name)
        self._watcher = None
        self._userWatcher = None
        self._hosts = hosts
        self._registryPath = self._makePath()
        self._client = None
        self._connect()

    def _makePath(self, endpoint=None):
        parts = [config.basePath, self._name]
        if endpoint:
            parts.append(endpoint)
        logging.debug('/'.join(parts))
        return '/'.join(parts)

    def _connect(self):
        """
        Start the ZooKeeper client, and ensure path existence
        """
        self._client = KazooClient(hosts=self._hosts)
        self._client.start()  # TODO: handle exceptions
        self._client.add_listener(self._stateListener)
        self._client.ensure_path(self._registryPath)  # TODO: handle exceptions
        self._watcher = self._client.ChildrenWatch(self._registryPath, func=self._childrenWatch)
        self._endpoints = set(self._client.get_children(self._registryPath))

    def _stateListener(self, state):
        """
        Handle cases where the connection to ZooKeeper disconnects, and the children update
        :param state: the current KazooState
        """
        if state == KazooState.CONNECTED:
            newChildren = set(self._client.get_children(self._registryPath))
            if self._endpoints != newChildren:
                self._childrenWatch(newChildren)

    def _childrenWatch(self, children):
        """
        Update _endpoints on changes, and call the user's callback function
        """
        self._endpoints = set(children)
        if self._userWatcher:
            self._userWatcher(self.get_endpoints())

    def register(self, endpoint):
        """
        Add an ephemeral zookeeper node for this endpoint
        :param endpoint: our own endpoint that is how other nodes will know us, in a "host:port" format
        """
        if isinstance(self._client, KazooClient):
            try:
                self._client.create(path=self._makePath(endpoint), ephemeral=True)
            except NodeExistsError:
                pass  # node already exists, nothing to do (?)
            # TODO: handle more exceptions
        else:
            pass  # TODO: handle missing client

    def watch(self, callback):
        """
        Start watching the registry for changes, with a watch callback
        :param callback: a function in the form of f([endpoint, endpoint, ...])
        """
        if hasattr(callback, '__call__'):
            self._userWatcher = callback
        else:
            raise ValueError('callback should be callable')