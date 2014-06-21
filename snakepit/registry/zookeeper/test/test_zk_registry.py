from __future__ import absolute_import
from unittest import TestCase
from threading import Event

from .. import ZKRegistry

__author__ = 'nati'


class ZKRegistryTestCase(TestCase):
    def setUp(self):
        self.registry_args = ('testRegistry', 'localhost:2181')
        self.endpoint_pair = '1.2.3.4:31337'
        self.endpoint_pair2 = '1.2.3.4:1337'
        self.zookeeper_sync_time = 3

        self.registry = ZKRegistry(*self.registry_args)
        self.zk_event = Event()
        self.zk_endpoints = None
        self.registry.watch(self._on_endpoints_changed)

    def _on_endpoints_changed(self, endpoints):
        self.zk_endpoints = endpoints
        self.zk_event.set()

    def wait_on_zookeeper(self, timeout=None):
        self.zk_event.wait(timeout or self.zookeeper_sync_time)
        self.zk_event.clear()

    def test_register(self):
        self.assertNotIn(self.endpoint_pair, self.registry.get_endpoints())
        self.registry.register(self.endpoint_pair)
        self.wait_on_zookeeper()
        self.assertIn(self.endpoint_pair, self.registry.get_endpoints())

    def test_un_register(self):
        self.registry.register(self.endpoint_pair)
        self.registry.stop()  # "UnRegister"
        self.wait_on_zookeeper()
        self.registry = ZKRegistry(*self.registry_args)
        self.assertNotIn(self.endpoint_pair, self.registry.get_endpoints())

    def test_watch(self):
        self.assertIsNone(self.zk_endpoints)
        self.registry.register(self.endpoint_pair)
        self.wait_on_zookeeper()
        self.assertIn(self.endpoint_pair, self.zk_endpoints)

    def test_multiple_endpoints(self):
        self.registry.register(self.endpoint_pair)
        self.wait_on_zookeeper()
        registry2 = ZKRegistry(*self.registry_args)
        registry2.register(self.endpoint_pair2)
        self.wait_on_zookeeper()
        self.assertEqual({self.endpoint_pair, self.endpoint_pair2}, set(self.registry.get_endpoints()))

    def tearDown(self):
        self.registry.stop()
        self.zk_event.clear()
