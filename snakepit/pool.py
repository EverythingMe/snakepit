from __future__ import absolute_import

import abc


class Pool(object):
    """
    This is the interface that all client pools should implement, regardless of the actual networking implementation
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def call(self, peer, method, *args, **kwargs):
        pass
