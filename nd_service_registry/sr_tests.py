import mock
import unittest

from kazoo.client import KazooState

import nd_service_registry


class KazooServiceRegistryTests(unittest.TestCase):
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
        # Simple checks should return the value of self.ndsr._state
        self.ndsr._state = True
        self.assertTrue(self.ndsr.get_state())
        self.ndsr._state = False
        self.assertFalse(self.ndsr.get_state())

    def test_get_state_with_callback(self):
        # With a callback, the callback should get executed
        callback_checker = mock.MagicMock()
        callback_checker.test.return_value = True

        # Mock the state to be True
        self.ndsr._state = True

        # Ensure that we return True, and that the callback is in the callbacks
        self.assertTrue(self.ndsr.get_state(callback_checker.test))
        self.assertTrue(callback_checker.test in self.ndsr._state_callbacks)
        callback_checker.test.assert_called_once_with(True)

    def test_state_callback_with_updated_state(self):
        # With a callback, the callback should get executed
        callback_checker = mock.MagicMock()
        callback_checker.test.return_value = True

        # Mock the state to be True
        self.ndsr._state = True

        # Add our callback checker mock above and validate that the callback
        # was executed once with True.
        self.ndsr.get_state(callback_checker.test)
        self.assertTrue(callback_checker.test in self.ndsr._state_callbacks)
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
