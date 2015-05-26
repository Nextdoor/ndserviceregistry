#!/usr/bin/env python
""" Copyright 2014 Nextdoor.com, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""

import uuid
import time

from kazoo.testing import KazooTestHarness
from nd_service_registry import KazooServiceRegistry
from nd_service_registry.lock import Lock


class LockTests(KazooTestHarness):

    # A flag for filtering nose tests
    integration = True

    def setUp(self):
        self.setup_zookeeper()
        self.server = 'localhost:20000'
        self.sandbox = "/tests/locks-%s" % uuid.uuid4().hex
        nd = KazooServiceRegistry(server=self.server,
                                  rate_limit_calls=0,
                                  rate_limit_time=0)
        self.zk = nd._zk

    def tearDown(self):
        self.teardown_zookeeper()

    def test_blocking_lock_with(self):
        lock1 = Lock(zk=self.zk, path=self.sandbox, name='lock1',
                     simultaneous=1, wait=1)

        with lock1 as s:
            status = s

        self.assertTrue(status)
        lock1.release()

    def test_non_blocking_lock_with(self):
        lock1 = Lock(zk=self.zk, path=self.sandbox, name='lock1',
                     simultaneous=1, wait=1)
        lock1.acquire()

        lock2 = Lock(zk=self.zk, path=self.sandbox, name='lock2',
                     simultaneous=1, wait=1)

        with lock2 as s:
            status = s

        self.assertFalse(status)
        lock1.release()

    def test_non_blocking_lock(self):
        # Get our first lock object at our path and acquire it.
        lock1 = Lock(zk=self.zk, path=self.sandbox, name='lock1',
                     simultaneous=1, wait=0)
        lock1.acquire()
        self.assertTrue(lock1.status())

        # Get our first lock object at our path and acquire it.
        lock2 = Lock(zk=self.zk, path=self.sandbox, name='lock2',
                     simultaneous=1, wait=0)
        lock2.acquire()
        self.assertFalse(lock2.status())

        lock1.release()
        lock2.release()

    def test_waiting_blocking_lock_wait(self):
        # Get our first lock object at our path and acquire it.
        lock1 = Lock(zk=self.zk, path=self.sandbox, name='lock1',
                     simultaneous=1, wait=0)
        lock1.acquire()
        self.assertTrue(lock1.status())

        # Get our first lock object at our path and try to acquire it.
        begin = time.time()
        lock2 = Lock(zk=self.zk, path=self.sandbox, name='lock2',
                     simultaneous=1, wait=2)
        lock2.acquire()

        # Make sure that the lock was not successful, and returned within 1
        # second of our requested wait time.
        self.assertFalse(lock2.status())
        self.assertTrue((time.time() - begin) >= 2)

        # lock1.release()  # waiting for fix from Kazoo guys
        lock2.release()
