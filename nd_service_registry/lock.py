# Copyright 2013 Nextdoor.com, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Kazoo Zookeeper Lock Object

Copyright 2013 Nextdoor Inc."""

__author__ = 'matt@nextdoor.com (Matt Wise)'

import logging
import time

from kazoo import exceptions

log = logging.getLogger(__name__)


class Lock(object):
    """Creates a Kazoo Lock object for a given path."""

    def __init__(self, zk, path, name=None, simultaneous=1, wait=300):
        """Initializes our Lock object.

        args:
            zk: Kazoo Zookeeper Connection Object
            path: String path of the lock to get
            name: Optional string name of the final lock object
            simultaneous: Int number of simultaneous locks to allow at this path
            wait: Int number of seconds to wait for lock before returning
        """
        # Set our local variables
        self._zk = zk
        self._path = path
        self._name = name
        self._simultaneous = simultaneous
        self._wait = wait

        # Create the Lock object in Kazoo. This lock object is used later
        # by our methods, but does not actively lock anything right away.
        self._lock = self._zk.Semaphore(self._path, self._name, self._simultaneous)

    def acquire(self):
        """Returns the actual Lock object for direct use by a client.

        Note: This is a blocking call that may never return

        returns:
          True: Lock was acquired
          False: Lock was unable to be acquired
        """

        log.debug('[%s] Acquiring the lock... (waiting %s seconds)' %
                  (self._path, self._wait))

        try:
            self._lock.acquire(blocking=False)
        except exceptions.CancelledError:
            # This means that the Lock was canceled somewhere manually.
            # In this case we just retry because retrying will clear out
            # the canceled state within the kazoo.Semaphore object.
            self._lock.acquire(blocking=False)

        # Begin waiting for the lock to be acquired. Wait as long as we've been
        # asked to, and every tenth of as econd check the status of the lock.
        begin = time.time()
        while time.time() - begin <= self._wait:
            if self._lock.is_acquired:
                log.info('[%s] Lock acquired after %s(s)...' %
                         (self._path, int(time.time() - begin)))
                return self._lock.is_acquired
            self._zk.handler.sleep_func(0.1)

        # We're done waiting. Return the current status of the lock and move on.
        log.info('[%s] Waited %s(s). Returning lock status %s...' %
                 (self._path, int(time.time() - begin), self._lock.is_acquired))
        return self._lock.is_acquired

    def release(self):
        """Request to release the Lock

        returns:
          True: Lock was released sucessfully
          False: Lock release failed
        """

        log.debug('[%s] Releasing the lock...' % self._path)
        self._lock.cancel()
        return self._lock.release()

    def status(self):
        """Return lock status

        returns:
          True: Lock is acquired
          False: Lock is not acquired
        """

        return self._lock.is_acquired

    def __enter__(self):
        """Provides 'with <lock object>:' support"""
        return self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        """Provides 'with <lock object>:' support"""
        self.release()
