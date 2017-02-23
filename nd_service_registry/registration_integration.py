from __future__ import print_function
from __future__ import absolute_import

import uuid
import time

from kazoo import security
from kazoo.testing import KazooTestHarness
from six.moves import range

from nd_service_registry import KazooServiceRegistry
from nd_service_registry.registration import RegistrationBase
from nd_service_registry.registration import EphemeralNode
from nd_service_registry.registration import DataNode


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


class RegistrationBaseTests(KazooTestHarness):

    # A flag for filtering nose tests
    integration = True

    def setUp(self):
        self.setup_zookeeper()
        self.server = 'localhost:20000'
        self.sandbox = "/tests/registration-%s" % uuid.uuid4().hex
        nd = KazooServiceRegistry(server=self.server,
                                  rate_limit_calls=0,
                                  rate_limit_time=0)
        self.zk = nd._zk

    def tearDown(self):
        self.teardown_zookeeper()

    def test_init(self):
        path = '%s/unittest-init' % self.sandbox
        data = {'unittest': 'data'}
        reg1 = RegistrationBase(zk=self.zk, path=path, data=data)

        # Ensure that the initial state of the RegistrationBase object
        # includes the original supplied data, the encoded, and the
        # decoded data bits that will be used for comparison later.
        self.assertFalse(reg1._ephemeral)
        self.assertFalse(reg1._state)
        self.assertEquals(path, reg1._path)
        self.assertEquals(data, reg1._data)
        self.assertTrue(b'unittest' in reg1._encoded_data)
        self.assertTrue('unittest' in reg1._decoded_data)

        # The RegistrationBase object does not aggressively set the data
        # or path in Zookeeper at instantiation time, so the returned data
        # should be None.
        self.assertEquals(None, reg1._watcher.get()['data'])
        self.assertEquals(None, reg1._watcher.get()['stat'])

        # First, data() should return None because we havn't actively
        # registered the path.
        self.assertEquals(None, reg1.data())
        self.assertEquals({'path': path, 'stat': None,
                           'data': None, 'children': []}, reg1.get())
        self.assertFalse(reg1.state())
        self.assertEquals(None, self.zk.exists(path))

        # Tear down the object
        reg1.stop()

    def test_start(self):
        path = '%s/unittest-start' % self.sandbox
        data = {'unittest': 'data'}
        reg1 = RegistrationBase(zk=self.zk, path=path, data=data)

        # Now register the path and wait until reg1.data() returns some data
        # other than None. If it doesn't after 5 seconds, fail.
        reg1.start()
        waituntil(reg1.data, None, 5)

        # Now that some data is back, make sure that its correct in zookeeper,
        # and in the Registration object.
        self.assertTrue(self.zk.exists(path))
        data = self.zk.get(path)[0]
        self.assertTrue('created' in reg1.data() and b'created' in data)
        self.assertTrue('pid' in reg1.data() and b'pid' in data)
        self.assertTrue('unittest' in reg1.data() and b'unittest' in data)
        self.assertTrue(reg1.state())

        # Tear down the object
        reg1.stop()

    def test_update(self):
        path = '%s/unittest-update' % self.sandbox
        data = {'unittest': 'data'}
        reg1 = RegistrationBase(zk=self.zk, path=path, data=data)

        # Now register the path and wait until reg1.data() returns some data
        # other than None. If it doesn't after 5 seconds, fail.
        reg1.start()
        waituntil(reg1.data, None, 5)

        # Test updating the data now that its registered
        current_data = reg1.data()
        reg1.set_data('foobar')
        waituntil(reg1.data, current_data, 5)
        self.assertEquals('foobar', reg1.data()['string_value'])
        self.assertTrue(b'foobar' in self.zk.get(path)[0])

        # Test disabling the node through the update() method
        current_data = reg1.data()
        reg1.update(None, False)
        waituntil(reg1.data, current_data, 5)
        self.assertFalse(reg1.state())
        self.assertEquals({'path': path, 'stat': None,
                           'data': None, 'children': []}, reg1.get())
        self.assertEquals(None, self.zk.exists(path))

    def test_start_acl_behavior(self):
        # Set up a username/password so an ACL is created
        ACL = security.make_digest_acl('user', 'password', all=True)
        self.zk.add_auth('digest', 'user:password')
        self.zk.default_acl = [ACL]

        # Test creating a nested path, and validate that the ACLs for
        # that neseted path are setup properly
        path = '%s/unit/test/host:22' % self.sandbox
        data = {}
        reg1 = RegistrationBase(zk=self.zk, path=path, data=data)

        # Now registre the path and wait for it to finish
        reg1.start()
        waituntil(reg1.data, None, 5)

        # Now, lets check the ACL for each path that would have
        # been created.
        self.assertEquals(
            security.OPEN_ACL_UNSAFE,
            self.zk.get_acls('%s/unit' % self.sandbox)[0])
        self.assertEquals(
            [ACL], self.zk.get_acls('%s/unit/test' % self.sandbox)[0])

    def test_stop(self):
        path = '%s/unittest-update' % self.sandbox
        data = {'unittest': 'data'}
        reg1 = RegistrationBase(zk=self.zk, path=path, data=data)

        # Now register the path and wait until reg1.data() returns some data
        # other than None. If it doesn't after 5 seconds, fail.
        reg1.start()
        waituntil(reg1.data, None, 5)
        self.assertTrue(self.zk.exists(path))

        # Now, test shutting the node down
        current_stat = reg1.get()
        reg1.stop()
        waituntil(reg1.get, current_stat, 5)
        self.assertEquals(None, reg1.get()['stat'])
        self.assertFalse(reg1.state())
        self.assertEquals(None, self.zk.exists(path))


class EphemeralNodeTests(KazooTestHarness):

    # A flag for filtering nose tests
    integration = True

    def setUp(self):
        self.setup_zookeeper()
        self.server = 'localhost:20000'
        self.sandbox = "/tests/ephemeral-%s" % uuid.uuid4().hex
        nd = KazooServiceRegistry(server=self.server,
                                  rate_limit_calls=0,
                                  rate_limit_time=0)
        self.zk = nd._zk

    def tearDown(self):
        self.teardown_zookeeper()

    def test_init(self):
        path = '%s/unittest-init' % self.sandbox
        data = {'unittest': 'data'}
        eph1 = EphemeralNode(zk=self.zk, path=path, data=data)
        waituntil(eph1.data, None, 5)

        # The EphemeralNode object DOES immediately register itself in
        # zookeeper, so we should be able to pull that data from Zookeeper
        # right away.
        (data, stat) = self.zk.get(path)
        self.assertNotEquals(None, stat)
        self.assertTrue(b'"unittest":"data"' in data)

    def test_greedy_ownership_of_data(self):
        path = '%s/unittest-greedy-data' % self.sandbox
        data = {'unittest': 'data'}
        eph1 = EphemeralNode(zk=self.zk, path=path, data=data)
        waituntil(eph1.data, None, 5)

        # Lets intentionally change the data in Zookeeper directly,
        # the EphemeralNode should immediately re-set the data.
        current_stat = eph1.get()
        self.zk.set(path, value=b'bogus')
        waituntil(eph1._watcher.get, current_stat, 5)
        (data, stat) = self.zk.get(path)
        self.assertIn(b'"unittest":"data"', data)
        self.assertNotIn(b'bogus', data)

        # Now lets intentionally delete the node and see it get re-run
        current_stat = eph1.get()
        self.zk.delete(path)
        waituntil(eph1._watcher.get, current_stat, 5)
        (data, stat) = self.zk.get(path)
        self.assertTrue(b'"unittest":"data"' in data)
        self.assertTrue(self.zk.exists(path))

    def test_greedy_ownership_of_state(self):
        path = '%s/unittest-greedy-state' % self.sandbox
        data = {'unittest': 'data'}
        eph1 = EphemeralNode(zk=self.zk, path=path, data=data)
        waituntil(eph1.data, None, 5)

        # Try disabling the node. If the node gets recreated automatically in
        # some way (by some rogue daemon), then we should destroy it.
        def path_exists_in_zk():
            if self.zk.exists(path):
                return True

            return False

        eph1.stop()
        self.assertEquals(None, self.zk.exists(path))
        self.zk.create(path)
        waituntil(path_exists_in_zk, True, 5)
        self.assertEquals(None, self.zk.exists(path))


class DataNodeTests(KazooTestHarness):

    # A flag for filtering nose tests
    integration = True

    def setUp(self):
        self.setup_zookeeper()
        self.server = 'localhost:20000'
        self.sandbox = "/tests/data-%s" % uuid.uuid4().hex
        nd = KazooServiceRegistry(server=self.server,
                                  rate_limit_calls=0,
                                  rate_limit_time=0)
        self.zk = nd._zk

    def tearDown(self):
        self.teardown_zookeeper()

    def test_init(self):
        path = '%s/unittest' % self.sandbox
        data = {'unittest': 'data'}
        DataNode(zk=self.zk, path=path, data=data)

        # The DataNode object DOES immediately register itself in
        # zookeeper, so we should be able to pull that data from Zookeeper
        # right away.
        (data, stat) = self.zk.get(path)
        self.assertNotEquals(None, stat)
        self.assertTrue(b'"unittest":"data"' in data)

    def test_delete_of_data_in_zk(self):
        path = '%s/unittest-delete' % self.sandbox
        data = {'unittest': 'data'}
        datanode = DataNode(zk=self.zk, path=path, data=data)

        # If the object is deleted in Zookeeper, our DataNode should reflect
        # that. It should also be able to re-update the value when told to.
        def get_stat_from_watcher():
            return datanode._watcher.get()['stat']

        # First, delete the path
        self.zk.delete(path)
        waituntil(get_stat_from_watcher, None, 5, mode=2)
        # Validate that the path is deleted, and the DataNode object was
        # updated correctly
        self.assertEquals(None, datanode.get()['data'])
        self.assertEquals(None, datanode.get()['stat'])

    def test_update(self):
        path = '%s/unittest-update' % self.sandbox
        data = {'unittest': 'data'}
        datanode = DataNode(zk=self.zk, path=path, data=data)

        # First, delete the path
        def get_stat_from_watcher():
            return datanode._watcher.get()['stat']

        # Get the original stat data, then delete the path, then wait
        # for the get() method to not return the original stat data
        orig_stat = datanode._watcher.get()['stat']
        self.zk.delete(path)
        waituntil(get_stat_from_watcher, orig_stat, 5, mode=1)

        # Ok now call the update() method and see if it re-registers
        datanode.update(data=data, state=True)
        waituntil(get_stat_from_watcher, None, 5, mode=1)
        self.assertEquals(data, datanode._data)

    def test_updating_of_local_cache(self):
        path = '%s/unittest-update-cache' % self.sandbox
        data = {'unittest': 'data'}
        datanode = DataNode(zk=self.zk, path=path, data=data)

        # Resetting the data in ZK should not cause the DataNode object to do
        # anything but update its local cache of the data.
        def get_string_value_from_watcher():
            if not datanode._watcher.get()['data']:
                return False

            if 'string_value' in datanode._watcher.get()['data']:
                return datanode._watcher.get()['data']['string_value']

            return False

        self.zk.set(path, b'foobar')
        waituntil(get_string_value_from_watcher, 'foobar', 5, mode=2)
        self.assertEquals('foobar',
                          datanode._watcher.get()['data']['string_value'])
        self.assertEquals('foobar', datanode._data)

    def test_set_data(self):
        path = '%s/unittest-set-data' % self.sandbox
        datanode = DataNode(zk=self.zk, path=path, data=None)

        # Now lets ensure that if we call set_data() that the data in Zookeeper
        # is updated at least once. It should not be, though, updated multiple
        # times.
        def get_string_value_from_datanode():
            data = datanode.get()['data']

            if data is None:
                return False

            if 'string_value' in data:
                return data['string_value']

            return False

        datanode.set_data('foo')
        waituntil(get_string_value_from_datanode, 'foo', 5, mode=2)
        (data, stat) = self.zk.get(path)
        for i in range(1, 10):
            datanode.set_data('foo')
        (data2, stat2) = self.zk.get(path)
        self.assertEquals(stat, stat2)

        # Ok, calling set_data() with the same data over and over again only
        # updates Zookeeper once. Good. Now what happens if the data in
        # Zookeeper changes and we call
        def get_string_value_from_watcher():
            if 'string_value' in datanode._watcher.get()['data']:
                return datanode._watcher.get()['data']['string_value']
            return False

        datanode.set_data('foo')
        (data, stat) = self.zk.get(path)
        self.zk.set(path, b'foobar')
        waituntil(get_string_value_from_watcher, 'foobar', 5, mode=2)
        datanode.set_data('foo')
        (data2, stat2) = self.zk.get(path)
        self.assertNotEquals(stat, stat2)
