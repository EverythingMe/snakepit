from __future__ import absolute_import

import socket
import struct
import fcntl
import sys
import abc


class TransportException(Exception):
    pass


class RemoteException(Exception):
    pass


class ServerTransport(object):
    """
    Base interface for all servers
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, handler, endpoint=None):
        self._handler = handler
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if endpoint is None:
            self._addr = self.get_default_interface_addr()
            self._socket.bind((self._addr, 0))
            self._port = self._socket.getsockname()[-1]
            self._endpoint = '{}:{}'.format(self._addr, self._port)
        else:
            self._endpoint = endpoint
            self._addr, port = self.endpoint.split(':')
            self._port = int(port)
            self._socket.bind((self._addr, self._port))

    @property
    def endpoint(self):
        return self._endpoint

    @staticmethod
    def get_default_gateway_interface_linux():
        """Read the default gateway directly from /proc."""
        with open('/proc/net/route') as fh:
            for line in fh:
                fields = line.strip().split()
                if fields[1] != '00000000' or not int(fields[3], 16) & 2:
                    continue

                return fields[0]

    def get_ip_address_linux(self, ifname):
        """Sets the socket's interface to `ifname` returns the socket's address"""
        return socket.inet_ntoa(fcntl.ioctl(
            self._socket.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', ifname[:15])
        )[20:24])

    def get_default_interface_addr(self):
        if sys.platform.startswith('linux'):
            iface = self.get_default_gateway_interface_linux()
            return self.get_ip_address_linux(iface)
        else:
            raise NotImplementedError(
                "Don't know how to find default interface address for platform: {}".format(sys.platform))

    def handle(self, method, *args, **kwargs):
        return getattr(self._handler, method)(*args, **kwargs)

    @abc.abstractmethod
    def listen(self):
        pass

    @abc.abstractmethod
    def stop(self):
        pass


class ClientTransport(object):
    """

    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def call(self, method, *args, **kwargs):
        pass
