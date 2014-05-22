__author__ = 'dvirsky'

import abc

class ServerTransport(object):
    """
    Base interface for all servers
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, endpoint, handler):
        self._handler = handler
        self._endpoint = endpoint

    @property
    def handler(self):
        return self._handler

    @property
    def endpoint(self):
        return self._endpoint

    @abc.abstractmethod
    def listen(cls, addr, handler):
        pass

class ClientTransport(object):
    """

    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def call(self, callName, *args, **kwargs):
        pass


