from __future__ import absolute_import

import mock
import unittest

from six.moves import range

from nd_service_registry import watcher


class WatcherTests(unittest.TestCase):

    # A flag for filtering nose tests
    unit = True

    @mock.patch('kazoo.recipe.watchers.DataWatch')
    @mock.patch('kazoo.recipe.watchers.ChildrenWatch')
    def setUp(self, children_watch, data_watch):
        # During the setup we mock out the actual KazooClient
        # object, as well as the DataWatch and ChildrenWatch classes
        # that are used directly by the Watcher class.
        self.zk = mock.MagicMock()
        self.path = '/test'
        self.children_watch = children_watch
        self.data_watch = data_watch

        # Register a MagicMock that will be used to trap callbacks from
        # our Watcher during the tests.
        self.callback_watcher = mock.MagicMock()
        self.callback_watcher.test.return_value = True

        # Create the Watcher
        self.watch = watcher.Watcher(
            self.zk,
            self.path,
            callback=self.callback_watcher.test)

    def test_init(self):
        # Ensure that the Watcher has been initialized properly
        self.assertTrue(self.callback_watcher.test in self.watch._callbacks)
        self.assertEquals(self.watch._current_data_watch,
                          self.data_watch.return_value)

    def test_execute_callbacks(self):
        # Reset any knowledge of calls to the callback from the setUp()
        self.callback_watcher.reset_mock()

        # Execute the callback method several times. It sohuld only fire off
        # once
        for i in range(0, 5):
            self.watch._execute_callbacks()

        # Executing the callbacks should happen only when the data changes.
        self.assertEquals(1, self.callback_watcher.test.call_count)

    def test_update_with_stat_and_data(self):
        # Tell the Watcher that the node exists with some real-looking
        # stat/data parameters.
        fake_data_str = 'unittest'
        expected_decoded_fake_data = {'string_value': 'unittest'}
        fake_stat = 'stat'
        self.watch._update(fake_data_str, fake_stat)

        # Ensure that we've updated the local object data appropriately
        self.assertEquals(self.watch._data, expected_decoded_fake_data)

        # Also guarantee that the callbacks were executed
        self.callback_watcher.test.assert_any_call

        # Finally, make sure we stored the fact that a ChildrenWatch
        # object was created by the _update() function.
        self.assertTrue(self.watch._current_children_watch is not None)

    def test_update_with_nonexistent_node(self):
        # Start out by telling the Watcher that the node doesn't exist
        # by passing None in for the 'data' and 'stat'.
        fake_data_str = None
        expected_decoded_fake_data = None
        fake_stat = None
        self.watch._update(fake_data_str, fake_stat)

        # Ensure that w've updated the lcoal object data appropriately
        self.assertEquals(self.watch._data, expected_decoded_fake_data)

        # Also guarantee that the callbacks were executed
        self.callback_watcher.test.assert_any_call

        # In this case, verify that we did NOT attempt to create a
        # ChildrenWatch object because the path does not exist.
        self.assertTrue(self.watch._current_children_watch is None)

    def test_update_children(self):
        # In this test, self.zk.retry() should never be called so we don't
        # hae to mock it out. Just supply a list of children, and this list
        # should be stored and then the callbacks should be executed.
        fake_children = ['child2', 'child1']
        self.watch._update_children(fake_children)

        # Ensure that the hash returned has a sorted list of children
        self.assertEquals(self.watch._children, sorted(fake_children))

        # Also ensure that we never called self.zk.retry()
        self.zk.retry.assert_has_calls([])
