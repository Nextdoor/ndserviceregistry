from __future__ import print_function
from __future__ import absolute_import

import uuid
import mock
import time

from kazoo.testing import KazooTestHarness
from six.moves import range

from nd_service_registry import KazooServiceRegistry
from nd_service_registry.watcher import Watcher


def waituntil(predicate, predicate_value, timeout, period=0.1, mode=1):
    mustend = time.time() + timeout
    while time.time() < mustend:
        if mode == 1:
            comparison = predicate() != predicate_value
        else:
            comparison = predicate() == predicate_value

        if comparison:
            print("Exiting timer, %s changed..." % predicate)
            return True
        print("Sleeping, waiting for %s to change..." % predicate)
        time.sleep(period)
    raise Exception('Failed waiting for %s to change...' % predicate)


class WatcherIntegrationTests(KazooTestHarness):

    # A flag for filtering nose tests
    integration = True

    def setUp(self):
        self.setup_zookeeper()
        self.server = 'localhost:20000'
        self.sandbox = "/tests/watcher-%s" % uuid.uuid4().hex
        nd = KazooServiceRegistry(server=self.server,
                                  rate_limit_calls=0,
                                  rate_limit_time=0)
        self.zk = nd._zk

    def tearDown(self):
        self.teardown_zookeeper()

    def test_watch_on_existing_path(self):
        path = '%s/existing-path-%s' % (self.sandbox, uuid.uuid4().hex)
        self.zk.create(path, makepath=True)
        watch = Watcher(zk=self.zk, path=path)

        # We expect there to be valid data...
        self.assertEquals(None, watch.get()['data'])
        self.assertNotEquals(None, watch.get()['stat'])
        self.assertEquals([], watch.get()['children'])

        # Now register a child node, and expect the data to change
        child_path = '%s/my_child_test' % path
        self.zk.create(child_path)

        # Before progressing, wait for Zookeeper to have kicked off
        # all of its notifications to the client.
        def get_children():
            print("got: %s" % watch.get()['children'])
            return watch.get()['children']
        waituntil(get_children, [], timeout=5)

        # Now expect the watch to be kicked off
        self.assertTrue('my_child_test' in watch.get()['children'])

    def test_watch_on_nonexistent_path_with_data_change(self):
        path = '%s/nonexistent-path-%s' % (self.sandbox, uuid.uuid4().hex)
        watch = Watcher(zk=self.zk, path=path)

        # Initially, we expect the data, children count and stat data
        # to be None because the node doesn't exist.
        self.assertEquals(None, watch.get()['data'])
        self.assertEquals(None, watch.get()['stat'])
        self.assertEquals([], watch.get()['children'])

        # Now, lets go and set the data path with something. We expect the
        # watcher now to actually register the change. Use the waituntil()
        # method to wait until the data has been updated.
        def get_data():
            return watch.get()['data']

        self.zk.create(path, value=b"{'foo': 'bar'}", makepath=True)
        waituntil(get_data, None, timeout=5, mode=1)

        # Now verify that the data has been updated in our Watcher
        self.assertEquals(
            {'string_value': "{'foo': 'bar'}"}, watch.get()['data'])
        self.assertNotEquals(None, watch.get()['stat'])
        self.assertEquals([], watch.get()['children'])

    def test_watch_on_nonexistent_path_with_children_change(self):
        path = '%s/nonexistent-path-%s' % (self.sandbox, uuid.uuid4().hex)
        watch = Watcher(zk=self.zk, path=path)

        # Initially, we expect the data, children count and stat data
        # to be None because the node doesn't exist.
        self.assertEquals(None, watch.get()['data'])
        self.assertEquals(None, watch.get()['stat'])
        self.assertEquals([], watch.get()['children'])

        # Now register a child node under the path, and expect the
        # data to change
        child_path = '%s/my_child_test' % path
        self.zk.create(child_path, makepath=True)

        # Wait until the children list changes
        def get_children():
            return watch.get()['children']
        waituntil(get_children, [], timeout=5)

        # Now verify that the data has been updated in our Watcher
        self.assertTrue('my_child_test' in watch.get()['children'])

    def test_watch_on_nonexistent_path_with_multiple_data_changes(self):
        path = '%s/nonexistent-path-%s' % (self.sandbox, uuid.uuid4().hex)
        watch = Watcher(zk=self.zk, path=path)

        # In order to track the number of callbacks that are registered,
        # we register a mocked method in the callbacks.
        callback_checker = mock.Mock()
        callback_checker.test.return_value = True
        watch.add_callback(callback_checker.test)

        # Initially, we expect the data, children count and stat data
        # to be None because the node doesn't exist.
        self.assertEquals(None, watch.get()['data'])
        self.assertEquals(None, watch.get()['stat'])
        self.assertEquals([], watch.get()['children'])

        # Now register a child node under the path, and expect the
        # data to change
        child_path = '%s/my_child_test' % path
        self.zk.create(child_path, makepath=True)

        # Wait until the children list changes
        def get_children():
            return watch.get()['children']
        waituntil(get_children, [], timeout=5)

        # Now verify that the data has been updated in our Watcher
        self.assertTrue('my_child_test' in watch.get()['children'])

        # Now make a series of data changes and wait for each one to
        # take effect before moving on to the next one.
        def get_data():
            return watch.get()['data']
        for i in range(1, 5):
            self.zk.set(path, value=('%s' % i).encode('UTF-8'))
            waituntil(get_data, {'string_value': '%s' % i}, timeout=5, mode=2)

        # The right number of calls is 6. There are a few upfront calls when
        # the Watcher object is initialized, and after that there are a few
        # calls for the updated node-data when new children are added. The
        # call count would be MUCH higher (11++) if we were recursively
        # creating Watches though.
        self.assertEquals(6, len(callback_checker.test.mock_calls))

    def test_deleting_node_watcher_is_watching(self):
        # Create our test path
        path = '%s/path-to-be-deleted-%s' % (self.sandbox, uuid.uuid4().hex)
        self.zk.create(path, makepath=True)

        # Watch it and verify that we got back the proper stat data for
        # the existing node.
        watch = Watcher(zk=self.zk, path=path)
        self.assertTrue(watch.get()['stat'] is not None)

        # Now, delete it
        self.zk.delete(path)

        # Now, wait until the Zookeeper callback has been executed, at which
        # point, our Watcher object should cleanly update itself indicating
        # that the path no longer exists.
        expected_get_results = {
            'path': path, 'stat': None, 'data': None, 'children': []}
        waituntil(watch.get, expected_get_results, 5, mode=2)
        self.assertEquals(expected_get_results, watch.get())

        # Note: In the background, Kazoo is likely throwing a NoNodeError
        # exception on one of its threads. This seems unavoidable right now,
        # but is also not a catchable Exception because of where it happens
        # in the code. This exception can be seen if you uncomment this line
        # below, which forces this test to fail and display all log messages.
        #
        # Open Bug Report: https://github.com/python-zk/kazoo/issues/149
        #
        # self.assertTrue(False)
