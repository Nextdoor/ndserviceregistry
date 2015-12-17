from __future__ import absolute_import

import mock
import unittest

from kazoo.client import KazooState
from kazoo.protocol.states import ZnodeStat

from nd_service_registry import watcher
from nd_service_registry import registration
import nd_service_registry


class KazooServiceRegistryTests(unittest.TestCase):
    """Behavioral tests for the KazooServiceRegistry.

    These tests verify the overall behavior of the entire class as used
    by potential clients.
    """

    # A flag for filtering nose tests
    unit = True

    @mock.patch('nd_service_registry.ZookeeperClient')
    def setUp(self, mocked_zookeeper):
        self.ndsr = nd_service_registry.KazooServiceRegistry()

    def tearDown(self):
        # The NDSR object is a singleton ... if its already setup, we need
        # to wipe its initalized state for our tests on each test-run.
        self.ndsr._initialized = False

    def test_get_state(self):
        # Simple checks should return the value of self.ndsr._conn_state
        self.ndsr._conn_state = True
        self.assertTrue(self.ndsr.get_state())
        self.ndsr._conn_state = False
        self.assertFalse(self.ndsr.get_state())

    def test_get_state_with_callback(self):
        # With a callback, the callback should get executed
        callback_checker = mock.MagicMock()
        callback_checker.test.return_value = True

        # Mock the state to be True
        self.ndsr._conn_state = True

        # Ensure that we return True, and that the callback is in the callbacks
        self.assertTrue(self.ndsr.get_state(callback_checker.test))
        self.assertTrue(
            callback_checker.test in self.ndsr._conn_state_callbacks)
        callback_checker.test.assert_called_once_with(True)

    def test_state_callback_with_updated_state(self):
        # With a callback, the callback should get executed
        callback_checker = mock.MagicMock()
        callback_checker.test.return_value = True

        # Mock the state to be True
        self.ndsr._conn_state = True

        # Add our callback checker mock above and validate that the callback
        # was executed once with True.
        self.ndsr.get_state(callback_checker.test)
        self.assertTrue(
            callback_checker.test in self.ndsr._conn_state_callbacks)
        callback_checker.test.assert_called_with(True)

        # Now fake a state change to LOST
        self.ndsr._state_listener(KazooState.LOST)

        # Now validate that the callback was executed once with False when
        # we updated the state
        callback_checker.test.assert_called_with(False)

        # Now fake a state change to LOST
        self.ndsr._state_listener(KazooState.SUSPENDED)

        # Now validate that the callback was executed once with False when
        # we updated the state
        callback_checker.test.assert_called_with(False)

    def test_get_dummywatcher(self):
        # With a callback, the callback should get executed
        callback_checker = mock.MagicMock()
        callback_checker.test.return_value = True

        returned_watcher = self.ndsr._get_dummywatcher(
            '/foo', callback=callback_checker.test)
        self.assertEquals(type(returned_watcher), watcher.DummyWatcher)

        expected_data = {
            'path': '/foo',
            'stat': None,
            'data': None,
            'children': None}
        callback_checker.test.assert_called_with(expected_data)

    def test_get_with_no_zk_connection_or_cache(self):
        # With a callback, the callback should get executed
        callback_checker = mock.MagicMock()
        callback_checker.test.return_value = True

        # Disable the zookeeper connection
        self.ndsr.stop()
        self.ndsr._zk.connected = False

        expected_data = {
            'path': '/foo',
            'stat': None,
            'data': None,
            'children': None}

        returned_data = self.ndsr.get('/foo', callback=callback_checker.test)
        callback_checker.test.assert_called_with(expected_data)
        self.assertFalse(returned_data)
        self.assertTrue('/foo' in self.ndsr._watchers)

    def test_unset(self):
        # Default options
        with mock.patch.object(self.ndsr, 'set') as mock_method:
            self.ndsr.unset('/foobar')
        mock_method.assert_called_with(
            node='/foobar', data=None, state=False,
            type=registration.DataNode)

        # With custom node type set (though, this is rare)
        with mock.patch.object(self.ndsr, 'set') as mock_method:
            self.ndsr.unset('/foobar', type=registration.EphemeralNode)
        mock_method.assert_called_with(
            node='/foobar', data=None, state=False,
            type=registration.EphemeralNode)


class KazooServiceRegistryCachingTests(unittest.TestCase):
    """KazooServiceRegistry Caching Tests."""

    # A flag for filtering nose tests
    unit = True

    @mock.patch('nd_service_registry.ZookeeperClient')
    def setUp(self, mocked_zookeeper):
        self.ndsr = nd_service_registry.KazooServiceRegistry()

    def tearDown(self):
        # The NDSR object is a singleton ... if its already setup, we need
        # to wipe its initalized state for our tests on each test-run.
        self.ndsr._initialized = False

    @mock.patch('nd_service_registry.funcs.save_dict')
    def test_save_watcher_to_dict(self, mocked_save_dict):
        # A normal data node storing some real data should be cachable
        fake_node_data = {
            'children': {},
            'data': {'some': 'data'},
            'path': '/mydata',
            'stat': ZnodeStat(
                czxid=505, mzxid=505, ctime=1355719651687,
                mtime=1355719651687, version=0, cversion=1,
                aversion=0, ephemeralOwner=0, dataLength=0,
                numChildren=0, pzxid=506)}

        self.ndsr._cachefile = True
        self.ndsr._save_watcher_to_dict(fake_node_data)
        mocked_save_dict.assert_called_with(
            {'/mydata': fake_node_data}, True)

    @mock.patch('nd_service_registry.funcs.save_dict')
    def test_save_watcher_to_dict_with_empty_data(self, mocked_save_dict):
        # A data node though that has NO DATA is assumed to be a path used
        # only for storing ephemeral node lists (server registrations).
        # In this case, if the children-list AND data-list are empty, we
        # do not cache this.
        fake_node_data = {
            'children': {},
            'data': None,
            'path': '/services/ssh',
            'stat': ZnodeStat(
                czxid=505, mzxid=505, ctime=1355719651687,
                mtime=1355719651687, version=0, cversion=1,
                aversion=0, ephemeralOwner=0, dataLength=0,
                numChildren=0, pzxid=506)}

        self.ndsr._cachefile = True
        self.ndsr._save_watcher_to_dict(fake_node_data)
        self.assertEqual(mocked_save_dict.mock_calls, [])

    @mock.patch('nd_service_registry.funcs.save_dict')
    def test_save_watcher_to_dict_with_bogus_node(self, mocked_save_dict):
        # A data node though that has NO DATA is assumed to be a path used
        # only for storing ephemeral node lists (server registrations).
        # In this case, if the children-list AND data-list are empty, we
        # do not cache this.
        fake_node_data = {
            'children': {},
            'data': None,
            'path': '/services/ssh',
            'stat': None}

        self.ndsr._cachefile = True
        self.ndsr._save_watcher_to_dict(fake_node_data)
        self.assertEqual(mocked_save_dict.mock_calls, [])

    @mock.patch('nd_service_registry.funcs.save_dict')
    def test_save_watcher_to_dict_with_disabled_cache(self, mocked_save_dict):
        # If caching is disabled, it should return quickly
        fake_node_data = {
            'children': {},
            'data': None,
            'path': '/services/ssh',
            'stat': ZnodeStat(
                czxid=505, mzxid=505, ctime=1355719651687,
                mtime=1355719651687, version=0, cversion=1,
                aversion=0, ephemeralOwner=0, dataLength=0,
                numChildren=0, pzxid=506)}

        self.ndsr._cachefile = False
        self.ndsr._save_watcher_to_dict(fake_node_data)
        self.assertEqual(mocked_save_dict.mock_calls, [])
