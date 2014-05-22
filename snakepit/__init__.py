__author__ = 'dvirsky'

from collections import namedtuple

class Node(object):


    pass



class Handler(object):

    def do(self, callName, *args, *kwargs):
        pass

import hash_ring

class Node(object):

    def __init__(self, registry, handler):

        self._client = client
        self._listen()

        registry.register(self.endpoint)
        registry.watch(self._onPeersChange)

    def _listen(self):
        pass

    def _onPeersChange(self, endpoints):
        pass

    def _getPeer(self, key):
        pass

    def do(self, callName, args, kwargs):

        key = self._makeKey(callName, *args, **kwargs)

        if this_is_us:
            return self._client.do(callName, *args, **kwargs)
        else:
            return self._getPeer(key).do(callName, *args, **kwargs)



class Endpoint(namedtuple("Endpoint", ('host', 'port'))):

    @classmethod
    def fromString(cls, ep):
        """
        Init an endpoint from a host:port string
        :return: new instance of an Endpoint
        """

        h,p=ep.split(':')
        return cls(h, int(p))


class Registry(object):

    def __init__(self, name):
        self._name = name
        self._nodes = []

    def getNodes(self):
        """
        Return all the nodes in the registry
        :return: a list of endpoint strings
        """
        return tuple(self._nodes)

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

