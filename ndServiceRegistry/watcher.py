#!/usr/bin/python
#
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

from ndServiceRegistry import funcs

# For KazooServiceRegistry Class
import kazoo.exceptions

# Our default variables
from version import __version__ as VERSION

TIMEOUT = 30


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
            }
        }
    """

    LOGGING = 'ndServiceRegistry.Watcher'

    def __init__(self, zk, path, callback=None, watch_children=True):
        # Create our logger
        self.log = logging.getLogger('%s.%s' % (self.LOGGING, path))

        # Set our local variables
        self._zk = zk
        self._path = path
        self._watch_children = watch_children

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

        # When self._trigger is False, the _execute_callbacks() function will
        # just return True. Only if True will it actually execute the func.
        self._trigger = False

        # Start up
        self._begin()

    def get(self):
        """Returns local data/children in specific dict format"""
        ret = {}
        ret['stat'] = self.stat()
        ret['data'] = self.data()
        ret['children'] = self._children
        return ret

    def data(self):
        """Returns self._data"""
        return self._data

    def stat(self):
        """Returns self._stat"""
        return self._stat

    def children(self):
        """Returns self._children"""
        return self._children

    def path(self):
        """Returns self._path"""
        return self._path

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
                self.log.warning('Callback [%s] already exists. Not '
                                 'triggering again.' % callback)
                return

        self._callbacks.append(callback)
        callback(self.get())

    def _begin(self):
        # First, register a watch on the data for the path supplied.
        self.log.debug('Registering watch on data changes')
        @self._zk.DataWatch(self._path, allow_missing_node=True)
        def _update_root_data(data, stat):
            self.log.debug('Data change detected')
            trigger = False

            # Just a bit of logging
            if not stat:
                self.log.info('Node is not registered.')

            # Since we set allow_missing_node to True, the 'data' passed back
            # is ALWAYS 'None'. This means that we need to actually go out and
            # explicitly get the data whenever this function is called. As
            # long as 'stat' is not None, we know the node exists so this will
            # succeed.
            if stat:
                data, stat = self._zk.retry(self._zk.get, self._path)

            # If 'stat' is differnt than self._Stat, we need to trigger
            # the callbacks
            if not self._stat == stat:
                # Update our 'stat' as well. If its 'none', our users will know
                # that the node is NOT registered.
                trigger = True
                self._stat = stat

            # We only trigger our callbacks if something meaningfull has
            # has changed. If self._data and data are the same, then
            # we keep our mouths shut.
            decoded_data = funcs.decode(data)
            if not self._data == decoded_data:
                # Update our local variables with the raw data from Zookeeper.
                # This data is either None, or a BaseString. If the node does
                # not exist at all, it will be None.
                self.log.debug('New data: %s' % decoded_data)
                self._data = decoded_data
                trigger = True
            self._execute_callbacks(trigger)

        # Only register a watch on the children if this path exists. If
        # it doesnt, we're assuming that you're watching a specific node
        # that may or may not be registered.
        if self._zk.exists(self._path) and self._watch_children:
            self.log.debug('Registering watch on child changes')
            @self._zk.ChildrenWatch(self._path)
            def _update_child_list(children):
                self.log.debug('Children have changed')
                trigger = False
                # We only trigger our callbacks if something meaningfull has
                # has changed. If self._children and children are the same, then
                # we keep our mouths shut.
                if not sorted(children) == self._children:
                    self.log.debug('New children: %s' % sorted(children))
                    for child in sorted(children):
                        self._watch_child_data(child) 
                    trigger = True
                self._execute_callbacks(trigger)

    def _watch_child_data(self, child):
        fullpath = '%s/%s' % (self._path, child)

        @self._zk.DataWatch(fullpath, allow_missing_node=False)
        def _update_child_data(data, stat):
            self.log.debug('%s: Checking data' % fullpath)
            decoded_data = funcs.decode(data)
            trigger = False

            # if data and stat are None, then the node has been deleted
            if not data and not stat and child in self._children:
                del self._children[child]
                trigger = True
                return

            # if this is the first time that we're adding this node,
            # then do not trigger the callbacks because the
            # ChildrenWatch will take care of that for us.
            if not child in self._children:
                self.log.debug('%s: New child with data: %s' % (fullpath, decoded_data))
                self._children[child] = decoded_data

            # lastly, if the node already exists and we're just changing
            # its data, DO trigger the callbacks.
            elif not self._children[child] == decoded_data:
                self.log.debug('%s: New data: %s' % (fullpath, decoded_data))
                self._children[child] = decoded_data
                trigger = True

            self._execute_callbacks(trigger)

    def _execute_callbacks(self, state=None):
        """Runs any callbacks that were passed to us for a given path.

        Args:
            path: A string value of the 'path' that has been updated. This
                  triggers the callbacks registered for that path only."""
        if not self.state():
            return

        for callback in self._callbacks:
            self.log.debug('Executing callback %s' % callback)
            callback(self.get())

