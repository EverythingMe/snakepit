__author__ = 'bergundy'


class SimpleKVHandler(object):
    def __init__(self, name, debug=True):
        self._name = name
        self._debug = debug
        self._kv = {}

    def get(self, key):
        if self._debug:
            print 'GET {} {}'.format(self._name, key)
        return self._kv.get(key, None)

    def set(self, key, value):
        if self._debug:
            print 'SET {} {} => {}'.format(self._name, key, value)
        self._kv[key] = value

    @staticmethod
    def hash_key(fn, key, *args, **kwargs):
        return key
