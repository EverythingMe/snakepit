__author__ = 'dvirsky'


class BaseHandler(object):


    def do(self, callName, *args, **kwargs):

        return getattr(self, callName,)(*args, **kwargs)


