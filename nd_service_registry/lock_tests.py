import nose
import uuid
import unittest
import logging
import time

from nd_service_registry import KazooServiceRegistry
from nd_service_registry.lock import Lock


class LockTests(unittest.TestCase):
    def setUp(self):
        self.server = 'localhost:2182'
        self.sandbox = "/tests/locks-%s" % uuid.uuid4().hex

    def test_blocking_lock_with(self):
        """Make sure that the enter/exit functionality works."""
        nd = KazooServiceRegistry(server=self.server)
        lock1 = Lock(zk=nd._zk, path=self.sandbox, name='lock1',
                     simultaneous=1, wait=1)

        with lock1 as s:
            status = s

        self.assertTrue(status)
        lock1.release()

    def test_non_blocking_lock_with(self):
        """Make sure that the enter/exit functionality works in non-blocking mode."""
        nd = KazooServiceRegistry(server=self.server)
        lock1 = Lock(zk=nd._zk, path=self.sandbox, name='lock1',
                     simultaneous=1, wait=1)
        lock1.acquire()

        lock2 = Lock(zk=nd._zk, path=self.sandbox, name='lock2',
                     simultaneous=1, wait=1)

        with lock2 as s:
            status = s

        self.assertFalse(status)
        lock1.release()

    def test_non_blocking_lock(self):
        """Test that a non-blocking lock works"""
        nd = KazooServiceRegistry(server=self.server)

        # Get our first lock object at our path and acquire it.
        lock1 = Lock(zk=nd._zk, path=self.sandbox, name='lock1', simultaneous=1, wait=0)
        lock1.acquire()
        self.assertTrue(lock1.status())

        # Get our first lock object at our path and acquire it.
        lock2 = Lock(zk=nd._zk, path=self.sandbox, name='lock2', simultaneous=1, wait=0)
        lock2.acquire()
        self.assertFalse(lock2.status())

        lock1.release()
        lock2.release()

    def test_waiting_blocking_lock_wait(self):
        """Test that a blocking Lock works"""
        nd = KazooServiceRegistry(server=self.server)

        # Get our first lock object at our path and acquire it.
        lock1 = Lock(zk=nd._zk, path=self.sandbox, name='lock1', simultaneous=1, wait=0)
        lock1.acquire()
        self.assertTrue(lock1.status())

        # Get our first lock object at our path and try to acquire it.
        begin = time.time()
        lock2 = Lock(zk=nd._zk, path=self.sandbox, name='lock2', simultaneous=1, wait=2)
        lock2.acquire()

        # Make sure that the lock was not sucessful, and returned within 1 second
        # of our requested wait time.
        self.assertFalse(lock2.status())
        self.assertTrue((time.time() - begin) >= 2)

        #lock1.release()  # waiting for fix from Kazoo guys
        lock2.release()
