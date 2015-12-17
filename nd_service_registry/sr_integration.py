from __future__ import absolute_import

import mock
import uuid

from kazoo import exceptions
from kazoo.testing import KazooTestHarness

from nd_service_registry import KazooServiceRegistry


class KazooServiceRegistryIntegrationTests(KazooTestHarness):
    # A flag for filtering nose tests
    integration = True

    def setUp(self):
        self.setup_zookeeper()
        self.sandbox = "/tests/sr-%s" % uuid.uuid4().hex
        self.server = 'localhost:20000'
        self.ndsr = KazooServiceRegistry(server=self.server,
                                         rate_limit_calls=0,
                                         rate_limit_time=0)

    def tearDown(self):
        self.teardown_zookeeper()
        self.ndsr._initialized = False

    def test_get_state(self):
        self.ndsr.start()
        self.assertTrue(self.ndsr.get_state())

        self.ndsr.stop()
        self.assertFalse(self.ndsr.get_state())

    def test_get_state_with_callback(self):
        # With a callback, the callback should get executed
        callback_checker = mock.MagicMock()
        callback_checker.test.return_value = True

        self.ndsr.start()
        self.ndsr.get_state(callback_checker.test)
        self.ndsr.stop()

        self.assertTrue(mock.call(True) in callback_checker.test.mock_calls)
        self.assertTrue(mock.call(False) in callback_checker.test.mock_calls)

    def test_unset_node(self):
        path = '%s/test_unset_node' % self.sandbox
        self.ndsr.set_node(path)
        self.ndsr.unset(path)
        self.assertRaises(exceptions.NoNodeError,
                          self.ndsr._zk.get, path)

    def test_unset_data(self):
        path = '%s/test_unset_data' % self.sandbox
        self.ndsr.set_data(path)
        self.ndsr.unset(path)
        self.assertRaises(exceptions.NoNodeError,
                          self.ndsr._zk.get, path)

    def test_unset_data_with_missing_reg_object(self):
        path = '%s/test_unset_data_missing_reg_object' % self.sandbox
        self.ndsr._zk.create(path, makepath=True)
        self.ndsr.unset(path)
        self.assertRaises(exceptions.NoNodeError,
                          self.ndsr._zk.get, path)

    def test_unset_data_on_absent_path(self):
        path = '%s/test_unset_data_on_absent_path' % self.sandbox
        self.ndsr.unset(path)
        self.assertRaises(exceptions.NoNodeError,
                          self.ndsr._zk.get, path)


class KazooServiceRegistryIntegrationTestsWithAuth(KazooTestHarness):
    # A flag for filtering nose tests
    integration = True

    def setUp(self):
        self.setup_zookeeper()
        self.sandbox = "/tests/sr-%s" % uuid.uuid4().hex
        self.server = 'localhost:20000'
        self.username = 'user'
        self.password = 'pass'
        self.ndsr = KazooServiceRegistry(server=self.server,
                                         username=self.username,
                                         password=self.password,
                                         rate_limit_calls=0,
                                         rate_limit_time=0)
        self._zk = self.ndsr._zk

    def tearDown(self):
        self.teardown_zookeeper()
        self.ndsr._initialized = False

    def test_set_node_with_acl(self):
        path = '%s/set_node_test_1' % self.sandbox
        self.ndsr.set_node(path, {})
        acls, znode_stat = self.ndsr._zk.get_acls(path)

        # ACLs are returned back in a slightly different format than
        # when we set them, so we have to dig into the return value
        # a bit for tests.
        self.assertEquals(self.ndsr._acl[0], acls[0])
        self.assertEquals(self.ndsr._acl[1], acls[1])
