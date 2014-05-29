import time
from unittest import TestCase

from snakepit.zookeeper.zk_registry import ZKRegistry

__author__ = 'nati'


class ZKRegistryTestCase(TestCase):
    def setUp(self):
        self.registryArgs = ('testRegistry', 'localhost:2181')
        self.endpointPair = '1.2.3.4:31337'
        self.endpointPair2 = '1.2.3.4:1337'
        self.zookeeperSyncTime = 3

        self.registry = ZKRegistry(*self.registryArgs)

    def testRegister(self):
        self.assertNotIn(self.endpointPair, self.registry.get_endpoints())
        self.registry.register(self.endpointPair)
        time.sleep(self.zookeeperSyncTime)
        self.assertIn(self.endpointPair, self.registry.get_endpoints())

    def testUnRegister(self):
        self.registry.register(self.endpointPair)
        self.registry._client.stop()  # "UnRegister"
        time.sleep(self.zookeeperSyncTime)
        self.registry = ZKRegistry(*self.registryArgs)
        self.assertNotIn(self.endpointPair, self.registry.get_endpoints())

    def testWatch(self):
        watchCalled = []  # using array to avoid the missing 'nonlocal' in Python 2.7

        def watcher(children):
            if not watchCalled:
                self.assertIn(self.endpointPair, children)
            else:
                self.assertIn(self.endpointPair2, children)
            watchCalled.append(True)
        self.registry.watch(watcher)
        self.assertFalse(watchCalled)
        self.registry.register(self.endpointPair)
        time.sleep(self.zookeeperSyncTime)
        self.assertTrue(watchCalled)
        self.registry.register(self.endpointPair2)

    def testMultipleEndpoints(self):
        self.registry.register(self.endpointPair)
        registry2 = ZKRegistry(*self.registryArgs)
        registry2.register(self.endpointPair2)
        time.sleep(self.zookeeperSyncTime)
        self.assertIn(self.endpointPair, self.registry.get_endpoints())
        self.assertIn(self.endpointPair2, self.registry.get_endpoints())

    def tearDown(self):
        self.registry._client.stop()
        time.sleep(self.zookeeperSyncTime)