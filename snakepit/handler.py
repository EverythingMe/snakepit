__author__ = 'dvirsky'
from tornado.concurrent import Future

class BaseHandler(object):


    def do(self, callName, *args, **kwargs):

        return getattr(self, callName,)(*args, **kwargs)


class AsyncHandler(BaseHandler):


    def do(self, callName, *args, **kwargs):
        res = super(AsyncHandler, self).do(callName, *args, **kwargs)
        if not isinstance(res, Future):
            future = Future()
            future.set_result(res)
            return future
        else:
            return res


