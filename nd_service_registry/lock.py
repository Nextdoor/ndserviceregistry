# Copyright 2012 Nextdoor.com, Inc.
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

Copyright 2012 Nextdoor Inc."""

__author__ = 'matt@nextdoor.com (Matt Wise)'

import logging

from kazoo.exceptions import CancelledError

log = logging.getLogger(__name__)


class Lock(object):
    """Creates a Kazoo Lock object for a given path."""

    def __init__(self, zk, path, name=None, simultaneous=1):
        # Set our local variables
        self._zk = zk
        self._path = path
        self._name = name
        self._simultaneous = simultaneous

        # Create the Lock object in Kazoo. This lock object is used later
        # by our methods, but does not actively lock anything right away.
        self._lock = self._zk.Semaphore(self._path, self._name, self._simultaneous)

    def acquire(self):
        """Returns the actual Lock object for direct use by a client.

        Note: This is a blocking call that may never return
        """

        log.debug('[%s] Acquiring the lock... (waiting indefinitely)' %
                  self._path)
        try:
            return self._lock.acquire()
        except CancelledError:
            # This means that the Lock was canceled somewhere manually,
            # which can happen for alot of reasons. Simplest thing here
            # is to just return False
            return False

    def release(self):
        """Request to release the Lock"""

        log.debug('[%s] Releasing the lock...' % self._path)
        self._lock.cancel()
        return self._lock.release()

    def __enter__(self):
        """Provides 'with <lock object>:' support"""
        self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        """Provides 'with <lock object>:' support"""
        self.release()
