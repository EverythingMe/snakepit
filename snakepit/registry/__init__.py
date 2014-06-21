from __future__ import absolute_import

import logging; logging = logging.getLogger(__name__)

__author__ = 'dvirsky'


class Registry(object):
    def __init__(self, name):
        self._name = name
        self._endpoints = set()

    def get_endpoints(self):
        """
        Return all the endpoints in the registry
        :return: a list of endpoint strings
        """
        return tuple(sorted(self._endpoints))

    def register(self, endpoint):
        """
        Register ourselves as a node in the registry
        :param endpoint: our own endpoint that is how other nodes will know us, in a "host:port" format
        :return:
        """

        raise NotImplementedError()

    def watch(self, callback):
        """
        Start watching the registry for changes, with a watch callback
        :param callback: a function in the form of f([endpoint, endpoint, ...])
        """
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()


class StaticRegistry(Registry):
    def __init__(self, name):
        super(StaticRegistry, self).__init__(name)
        self._watchers = set()

    def register(self, endpoint):
        self._endpoints.add(endpoint)
        logging.info("Added node %s, registry state now: %s", endpoint, self._endpoints)

        eps = self.get_endpoints()
        for watcher in self._watchers:
            watcher(eps)

    def watch(self, callback):

        if callback in self._watchers:
            raise RuntimeError("watcher %s already in set" % callback)

        if not callable(callable):
            raise ValueError("Not a callable!")

        self._watchers.add(callback)

    def stop(self):
        pass
