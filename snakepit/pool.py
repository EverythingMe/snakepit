from __future__ import absolute_import



class Pool(object):
    """
    This is the interface that all client pools should implement, regardless of the actual networking implementation
    """
    def call(self, peer, callName, *args, **kwargs):
        pass
