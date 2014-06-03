import mock
import unittest
import threading

from nd_service_registry import watcher


class WatcherTests(unittest.TestCase):

    # A flag for filtering nose tests
    unit = True

    @mock.patch('kazoo.recipe.watchers.DataWatch')
    @mock.patch('kazoo.recipe.watchers.ChildrenWatch')
    def setUp(self, children_watch, data_watch):
        self.zk = mock.MagicMock()
        self.zk.handler.lock_object.return_value = threading.Lock()
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
        self.assertEquals(self.watch._data_watch, self.data_watch.return_value)

    def test_execute_callbacks(self):
        # Reset any knowledge of calls to the callback from the setUp()
        self.callback_watcher.reset_mock()

        # Execute the callback method several times. It sohuld only fire off
        # once
        for i in xrange(0, 5):
            self.watch._execute_callbacks()

        # Executing the callbacks should happen only when the data changes.
        self.assertEquals(1, self.callback_watcher.test.call_count)

    def test_update_with_stat_and_data(self):
        fake_data_str = 'unittest'
        expected_decoded_fake_data = {'string_value': 'unittest'}
        fake_stat = 'stat'

        # Call the method
        self.watch._update(fake_data_str, fake_stat)
        self.assertEquals(self.watch._data, expected_decoded_fake_data)
        self.callback_watcher.test.assert_any_call
        self.assertTrue(self.watch._children_watch is not None)

    def test_update_with_nonexistent_node(self):
        fake_data_str = None
        expected_decoded_fake_data = None
        fake_stat = None

        # Call the method
        self.watch._update(fake_data_str, fake_stat)
        self.assertEquals(self.watch._data, expected_decoded_fake_data)
        self.callback_watcher.test.assert_any_call
        self.assertTrue(self.watch._children_watch is None)

    def test_update_children(self):
        # Mock the retry method to return back some fake data
        self.zk.retry.return_value = ('data', 'stat')
        fake_children = ['child2', 'child1']
        expected_children_dict = {
            'child1': {'string_value': 'data'},
            'child2': {'string_value': 'data'}
        }

        self.watch._update_children(fake_children)

        # Ensure that the hash returned has a sorted list of children
        self.assertEquals((self.watch._children.keys()), sorted(fake_children))
        self.assertEquals(self.watch._children, expected_children_dict)

    def test_update_children_with_getdata_disabled(self):
        # Mock the retry method to return back some fake data
        self.zk.retry.return_value = ('data', 'stat')
        fake_children = ['child2', 'child1']

        self.watch._get_children_data = False
        self.watch._update_children(fake_children)

        # Ensure that the hash returned has a sorted list of children
        self.assertEquals(self.watch._children, sorted(fake_children))
