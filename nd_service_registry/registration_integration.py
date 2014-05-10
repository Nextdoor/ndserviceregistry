import uuid
import time

from kazoo.testing import KazooTestHarness
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
            print "Exiting timer, %s changed..." % predicate
            return True
        print "Sleeping, waiting for %s to change..." % predicate
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
        path = '%s/unittest' % self.sandbox
        data = {'unittest': 'data'}
        reg1 = RegistrationBase(zk=self.zk, path=path, data=data)

        # Ensure that the initial state of the RegistrationBase object
        # includes the original supplied data, the encoded, and the
        # decoded data bits that will be used for comparison later.
        self.assertFalse(reg1._ephemeral)
        self.assertFalse(reg1._state)
        self.assertEquals(path, reg1._path)
        self.assertEquals(data, reg1._data)
        self.assertTrue('unittest' in reg1._encoded_data)
        self.assertTrue('unittest' in reg1._decoded_data)

        # The RegistrationBase object does not aggressively set the data
        # or path in Zookeeper at instantiation time, so the returned data
        # should be None.
        self.assertEquals(None, reg1._watcher.get()['data'])
        self.assertEquals(None, reg1._watcher.get()['stat'])

    def test_public_methods(self):
        path = '%s/unittest' % self.sandbox
        data = {'unittest': 'data'}
        reg1 = RegistrationBase(zk=self.zk, path=path, data=data)

        # First, data() should return None because we havn't actively
        # registered the path.
        self.assertEquals(None, reg1.data())
        self.assertEquals({'path': path, 'stat': None,
                           'data': None, 'children': {}}, reg1.get())
        self.assertFalse(reg1.state())
        self.assertEquals(None, self.zk.exists(path))

        # Now register the path and wait until reg1.data() returns some data
        # other than None. If it doesn't after 5 seconds, fail.
        reg1.start()
        waituntil(reg1.data, None, 5)

        # Now that some data is back, make sure that its correct in zookeeper,
        # and in the Registration object.
        self.assertTrue(self.zk.exists(path))
        data = self.zk.get(path)[0]
        self.assertTrue('created' in reg1.data() and 'created' in data)
        self.assertTrue('pid' in reg1.data() and 'pid' in data)
        self.assertTrue('unittest' in reg1.data() and 'unittest' in data)
        self.assertTrue(reg1.state())

        # Test updating the data now that its registered
        current_data = reg1.data()
        reg1.set_data('foobar')
        waituntil(reg1.data, current_data, 5)
        self.assertEquals('foobar', reg1.data()['string_value'])
        self.assertTrue('foobar' in self.zk.get(path)[0])

        # Test disabling the node through the update() method
        current_data = reg1.data()
        reg1.update(None, False)
        waituntil(reg1.data, current_data, 5)
        self.assertFalse(reg1.state())
        self.assertEquals({'path': path, 'stat': None,
                           'data': None, 'children': {}}, reg1.get())
        self.assertEquals(None, self.zk.exists(path))

        # Re-enable the node for the final test
        current_stat = reg1.get()
        reg1.start()
        waituntil(reg1.get, current_stat, 5)
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

    def test_init_and_behavior(self):
        path = '%s/unittest' % self.sandbox
        data = {'unittest': 'data'}
        eph1 = EphemeralNode(zk=self.zk, path=path, data=data)
        waituntil(eph1.data, None, 5)

        # The EphemeralNode object DOES immediately register itself in
        # zookeeper, so we should be able to pull that data from Zookeeper
        # right away.
        (data, stat) = self.zk.get(path)
        self.assertNotEquals(None, stat)
        self.assertTrue('"unittest":"data"' in data)

        # Now, lets intentionally change the data in ZOokeeper directly,
        # the EphemeralNode should immediately re-set the data.
        current_stat = eph1.get()
        self.zk.set(path, value='bogus')
        waituntil(eph1._watcher.get, current_stat, 5)
        (data, stat) = self.zk.get(path)
        self.assertTrue('"unittest":"data"' in data)
        self.assertFalse('bogus' in data)

        # Now lets intentionally delete the node and see it get re-run
        current_stat = eph1.get()
        self.zk.delete(path)
        waituntil(eph1._watcher.get, current_stat, 5)
        (data, stat) = self.zk.get(path)
        self.assertTrue('"unittest":"data"' in data)
        self.assertTrue(self.zk.exists(path))

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

    def test_init_and_behavior(self):
        path = '%s/unittest' % self.sandbox
        data = {'unittest': 'data'}
        data1 = DataNode(zk=self.zk, path=path, data=data)

        # The DatNode object DOES immediately register itself in
        # zookeeper, so we should be able to pull that data from Zookeeper
        # right away.
        (data, stat) = self.zk.get(path)
        self.assertNotEquals(None, stat)
        self.assertTrue('"unittest":"data"' in data)

        # Resetting the data in ZK should not cause the DataNode object to do
        # anything but update its local cache of the data.
        def get_string_value_from_watcher():
            if 'string_value' in data1._watcher.get()['data']:
                return data1._watcher.get()['data']['string_value']
            return False

        self.zk.set(path, 'foobar')
        waituntil(get_string_value_from_watcher, 'foobar', 5, mode=2)
        self.assertEquals('foobar',
                          data1._watcher.get()['data']['string_value'])
        self.assertEquals('foobar', data1._data['string_value'])

        # Now lets ensure that if we call set_data() that the data in Zookeeper
        # is updated at least once. It should not be, though, updated multiple
        # times.
        data1.set_data('foo')
        (data, stat) = self.zk.get(path)
        for i in xrange(1, 10):
            data1.set_data('foo')
        (data2, stat2) = self.zk.get(path)
        self.assertEquals(stat, stat2)

        # Ok, calling set_data() with the same data over and over again only
        # updates Zookeeper once. Good. Now what happens if the data in
        # Zookeeper changes and we call
        data1.set_data('foo')
        (data, stat) = self.zk.get(path)
        self.zk.set(path, 'foobar')
        waituntil(get_string_value_from_watcher, 'foobar', 5, mode=2)
        data1.set_data('foo')
        (data2, stat2) = self.zk.get(path)
        self.assertNotEquals(stat, stat2)
