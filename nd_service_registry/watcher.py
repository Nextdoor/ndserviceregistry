# Copyright 2014 Nextdoor.com, Inc.
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

Copyright 2014 Nextdoor Inc."""
from __future__ import absolute_import

import logging

from kazoo.recipe import watchers

from nd_service_registry import funcs

__author__ = 'matt@nextdoor.com (Matt Wise)'


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
        """Initialize the Watcher object and begin watching a path.

        The initialization of a Watcher object automatically registers a
        data watch in Zookeeper on the path specificed. Any and all
        callbacks supplied during this initialization are executed as soon
        as the data is returned by Zookeeper.

        Optionally, a subsequent child watch is created on the children
        of the supplied path and again these callbacks are executed
        any time Zookeeper tells us that the children have changed
        in any way.

        args:
            zk: A kazoo.client.KazooClient object
            path: The path in Zookeeper to watch
            callback: A function to call when the path data changes
            wach_children: Whether or not to watch the children
        """
        # Set our local variables
        self._zk = zk
        self._path = path
        self._watch_children = watch_children

        # Get a lock handler
        self._run_lock = self._zk.handler.lock_object()

        # Create local caches of the 'data' and the 'children' data
        self._children = []
        self._data = None
        self._stat = None

        # Keep track of our Watcher objects in Kazoo with local references
        self._current_data_watch = None
        self._current_children_watch = None

        # Array to hold any callback functions we're supposed to notify when
        # anything changes on this path
        self._callbacks = []
        if callback:
            self._callbacks.append(callback)

        # Quick copy of the data last used when executing callbacks -- used
        # to prevent duplicate callback executions due to multiple watches.
        self._last_callback_executed_with = None

        # if self._state is False, then even on a data change, our callbacks
        # do not run.
        self._state = True

        # Start up our DataWatch. This can be started on any path regardless of
        # whether it exists or not. ChildrenWatch objects though must be
        # started on only existing-paths -- so we do not create that object
        # here, but instead do it from with the self._update() method.
        log.debug('[%s] Registering DataWatch (%s)' %
                  (self._path, self._current_data_watch))
        self._current_data_watch = watchers.DataWatch(
            client=self._zk,
            path=self._path,
            func=self._update)

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

    def _update(self, data, stat):
        """Function executed by Kazoo upon data/stat changes for a path.

        This function is registered during the __init__ process of this
        Object with Kazoo. Upon the initial registration, and any subsequent
        changes to the 'data' or 'stat' of a path, this function is called
        again.

        This function is responsible for updating the local Watcher object
        state (its path data, stat data, etc) and triggering any callbacks
        that have been registered with this Watcher.

        args:
            data: The 'data' stored in Zookeeper for this path
            stat: The 'stat' data for this path
        """
        self._data = funcs.decode(data)
        self._stat = stat
        log.debug('[%s] Data change detected: %s, Stat: %s' %
                  (self._path, self._data, self._stat))

        # ChildrenWatches can only be applied to already existing paths
        # (Kazoo throws a NoNodeError otherwise). To prevent this NoNodeError
        # from being thrown, we only register an additional ChildrenWatch
        # in the event that 'stat' was not None.
        if self._watch_children and stat and not self._current_children_watch:
            log.debug('[%s] Registering ChildrenWatch' % self._path)
            self._current_children_watch = watchers.ChildrenWatch(
                client=self._zk,
                path=self._path,
                func=self._update_children)

        # Lastly, execute our callbacks
        self._execute_callbacks()

    def _update_children(self, children):
        """Function executed by Kazoo upon children changes for a path.

        This function is registered by the _update() method during and is
        executed by Kazoo upon any children changes to a path. Its responsible
        for updating the local list of children for the path, and executing
        the appropriate callbacks.

        args:
            children: The list of children returned by Kazoo
        """
        self._children = sorted(children)
        log.debug('[%s] Children change detected: %s' %
                  (self._path, self._children))
        self._execute_callbacks()

    def _execute_callbacks(self):
        """Runs any callbacks that were passed to us for a given path.

        Args:
            path: A string value of the 'path' that has been updated. This
                  triggers the callbacks registered for that path only.
        """

        # If the current data and the last-execution data are the same, then
        # assume our callback notification was bogus and don't run.
        if self._last_callback_executed_with == self.get():
            log.debug('[%s] Last callback data matches current data, not '
                      'executing callbacks again.' % self._path)
            return

        log.debug('[%s] execute_callbacks triggered' % self._path)
        if not self.state():
            log.debug('[%s] self.state() is False - not executing callbacks.'
                      % self._path)
            return

        for callback in self._callbacks:
            log.debug('[%s] Executing callback %s' % (self._path, callback))
            callback(self.get())

        # Store a "last called with" variable that we can check to prevent
        # unnecessary extra callback executions.
        self._last_callback_executed_with = self.get()


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
