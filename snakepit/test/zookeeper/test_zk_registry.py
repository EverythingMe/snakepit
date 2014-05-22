import time
from unittest import TestCase

from snakepit.zookeeeper.zk_registry import ZKRegistry

__author__ = 'nati'


class ZKRegistryTestCase(TestCase):
    def setUp(self):
        self.registryArgs = ('testRegistry', 'localhost:2181')
        self.endpointPair = '1.2.3.4:31337'
        self.endpointPair2 = '1.2.3.4:31337'
        self.zookeeperSyncTime = 3

        self.registry = ZKRegistry(*self.registryArgs)

    def testRegister(self):
        self.assertNotIn(self.endpointPair, self.registry.getEndpoints())
        self.registry.register(self.endpointPair)
        time.sleep(self.zookeeperSyncTime)
        self.assertIn(self.endpointPair, self.registry.getEndpoints())

    def testUnRegister(self):
        self.registry.register(self.endpointPair)
        self.registry._client.stop()  # "UnRegister"
        time.sleep(self.zookeeperSyncTime)
        self.registry = ZKRegistry(*self.registryArgs)
        self.assertNotIn(self.endpointPair, self.registry.getEndpoints())

    def testWatch(self):
        watchCalled = []  # using array to avoid the missing 'nonlocal' in Python 2.7

        def watcher(children):
            watchCalled.append(True)
            self.assertIn(self.endpointPair, children)
        self.registry.watch(watcher)
        self.assertFalse(watchCalled)
        self.registry.register(self.endpointPair)
        time.sleep(self.zookeeperSyncTime)
        self.assertTrue(watchCalled)

    def testMultipleEndpoints(self):
        self.registry.register(self.endpointPair)
        registry2 = ZKRegistry(*self.registryArgs)
        registry2.register(self.endpointPair2)
        time.sleep(self.zookeeperSyncTime)
        self.assertIn(self.endpointPair, self.registry.getEndpoints())
        self.assertIn(self.endpointPair2, self.registry.getEndpoints())

    def tearDown(self):
        self.registry._client.stop()
        time.sleep(self.zookeeperSyncTime)