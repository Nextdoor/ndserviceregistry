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

"""Kazoo Zookeeper znode watch object

Copyright 2012 Nextdoor Inc."""

__author__ = 'matt@nextdoor.com (Matt Wise)'

import logging
import threading
import time
import sys

from os.path import split

from nd_service_registry import funcs

# For KazooServiceRegistry Class
import kazoo.exceptions

# Our default variables
from version import __version__ as VERSION

TIMEOUT = 30

log = logging.getLogger(__name__)


class Watcher(object):
    """Watches a Zookeeper path for children and data changes.

    This object provides a way to access and monitor all of the data on a
    given Zookeeper node. This includes its own node data, its children, and
    their data. It is not recursive.

    The object always maintains a local cached copy of the current state of
    the supplied path. This state can be accessed at any time by calling
    the get() method. Data will be returned in this format:

        {
            'data': { 'foo': 'bar', 'abc': 123' },
            'stat': ZnodeStat(czxid=116, mzxid=4032, ctime=1355424939217,
                    mtime=1355523495703, version=5, cversion=1912, aversion=0,
                    ephemeralOwner=0, dataLength=9, numChildren=2, pzxid=8388),
            'children': {
                'node1:22': { 'data': 'value' },
                'node2:22': { 'data': 'value2' },
            },
            'path': '/services/foo',
        }
    """

    def __init__(self, zk, path, callback=None, watch_children=True):
        # Set our local variables
        self._zk = zk
        self._path = path
        self._watch_children = watch_children

        # Get a lock handler
        self._run_lock = self._zk.handler.lock_object()

        # Create local caches of the 'data' and the 'children' data
        self._children = {}
        self._data = None
        self._stat = None

        # Array to hold any callback functions we're supposed to notify when
        # anything changes on this path
        self._callbacks = []
        if callback:
            self._callbacks.append(callback)

        # if self._state is False, then even on a data change, our callbacks
        # do not run.
        self._state = True

        # Start up
        self._begin()

    def get(self):
        """Returns local data/children in specific dict format"""
        ret = {}
        ret['stat'] = self._stat
        ret['path'] = self._path
        ret['data'] = self._data
        ret['children'] = self._children
        return ret

    def stop(self):
        """Stops watching the path."""
        # Stop the main run() method
        self._state = False

    def start(self):
        """Starts watching the path."""
        # Stop the main run() method
        self._state = True

    def state(self):
        """Returns self._state"""
        return self._state

    def add_callback(self, callback):
        """Add a callback when watch is updated."""
        for existing_callback in self._callbacks:
            if callback == existing_callback:
                log.debug('[%s] Callback [%s] already exists. Not triggering '
                          'again.' % (self._path, callback))
                return

        self._callbacks.append(callback)
        callback(self.get())

    def _begin(self):
        # First, register a watch on the data for the path supplied.
        log.debug('[%s] Registering watch on data changes' % self._path)

        @self._zk.DataWatch(self._path, allow_missing_node=True)
        def _update_root_data(data, stat):
            log.debug('[%s] Data change detected' % self._path)

            # NOTE: Applies to Kazoo <= 0.9, fixed in >= 1.0
            #
            # Since we set allow_missing_node to True, the 'data' passed back
            # is ALWAYS 'None'. This means that we need to actually go out and
            # explicitly get the data whenever this function is called. Try to
            # get the data with zk.get(). If a NoNodeError is thrown, then
            # we know the host is not registered.
            try:
                data, self._stat = self._zk.retry(self._zk.get, self._path)
                log.debug('[%s] Node is registered.' % self._path)
            except kazoo.exceptions.NoNodeError:
                log.debug('[%s] Node is not registered.' % self._path)

            self._data = funcs.decode(data)
            self._stat = stat

            log.debug('[%s] Data: %s, Stat: %s' %
                      (self._path, self._data, self._stat))
            self._execute_callbacks()

        # Only register a watch on the children if this path exists. If
        # it doesnt, we're assuming that you're watching a specific node
        # that may or may not be registered.
        if self._zk.exists(self._path) and self._watch_children:
            log.debug('[%s] Registering watch on child changes' % self._path)

            @self._zk.ChildrenWatch(self._path)
            def _update_child_list(data):
                log.debug('[%s] New children: %s' % (self._path, sorted(data)))
                children = {}
                for child in data:
                    fullpath = '%s/%s' % (self._path, child)
                    data, stat = self._zk.retry(self._zk.get, fullpath)
                    children[child] = funcs.decode(data)
                self._children = children
                self._execute_callbacks()

    def _execute_callbacks(self):
        """Runs any callbacks that were passed to us for a given path.

        Args:
            path: A string value of the 'path' that has been updated. This
                  triggers the callbacks registered for that path only."""
        log.debug('[%s] execute_callbacks triggered' % self._path)

        if not self.state():
            log.debug('[%s] self.state() is False - not executing callbacks.'
                      % self._path)
            return

        for callback in self._callbacks:
            log.debug('[%s] Executing callback %s' % (self._path, callback))
            callback(self.get())


class DummyWatcher(Watcher):
    """Provides a Watcher-interface, without any actual Zookeeper connection.

    This object can store and return data just like a Watcher object, but
    has no connection at all to Zookeeper or any actual watches. This object
    is meant to be used in the event that Zookeeper is down and we still
    want to be able to return valid data.
    """

    def __init__(self, path, data, callback=None):
        # Set our local variables
        self._path = path
        self._data = data['data']
        self._stat = data['stat']
        self._children = data['children']

        # Array to hold any callback functions we're supposed to notify when
        # anything changes on this path
        self._callbacks = []
        if callback:
            self.add_callback(callback)

    def stop(self):
        """Stops watching the path (not applicable here)"""
        return

    def start(self):
        """Starts watching the path (not applicable here)"""
        return

    def state(self):
        """Returns state of the object (not applicable here)"""
        return True
