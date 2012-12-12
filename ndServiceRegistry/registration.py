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

"""Kazoo Zookeeper znode registration client

Copyright 2012 Nextdoor Inc."""

__author__ = 'matt@nextdoor.com (Matt Wise)'

import logging
import threading
import time

from ndServiceRegistry import funcs

# For KazooServiceRegistry Class
import kazoo.exceptions

# Our default variables
from version import __version__ as VERSION

TIMEOUT = 30


class Registration(threading.Thread):
    """An object that registers a znode with ZooKeeper.

    This object handles the initial registration, updating of registered
    data, and connection state changes from the supplied ServiceRegistry
    object.
    """

    LOGGING = 'ndServiceRegistry.Registration.EphemeralNode'

    def __init__(self, zk, path, data, state=True):
        # Initiate our thread
        super(Registration, self).__init__()

        # Create our logger
        self.log = logging.getLogger(self.LOGGING)

        # Set our local variables
        self._zk = zk
        self._path = path
        self._state = state
        self._event = threading.Event()

        # Encode and set our data
        self.set_data(data)

        # These threads can die with prejudice. Make sure that any time the
        # python interpreter exits, we exit immediately
        self.setDaemon(True)

        # Start up
        self.start()

    def data(self):
        """Returns self._data"""
        return self._data

    def set_data(self, data):
        """Sets self._data.

        Args:
            data: String or Dict of data to register with this object."""
        self._data = funcs.encode(data)

    def path(self):
        """Returns self._path"""
        return self._path

    def run(self):
        """Simple background thread that monitors our Zookeeper registration.

        We want our threads to respond to an outside Exception extremely
        quickly, however, we don't want to run our self.register() loop super
        often. What we do here is loop every half-second and check whether the
        last-time the self.register() method executed is greater than TIMEOUT.
        """

        last_run = 0
        while True and not self._event.is_set():
            self._event.wait(0.2)
            if time.time() - last_run >= TIMEOUT:
                self.log.debug('Executing self.register()')
                self.register()
                last_run = time.time()

        self.log.debug('Registration %s is exiting run() loop.' % self._path)

    def stop(self):
        """Stops the run() method."""
        # Let go of our _zk reference
        self._zk = None

        # Stop the main run() method
        self._event.set()

    def state(self):
        """Returns self._state"""
        return self._state

    def set_state(self, state):
        """Sets the state of our registration.

        Updates the internal 'wanted' state of this object. If True, we want
        to be actively registered with Zookeeper. If False, we want to make
        sure we're not registered with Zookeeper.

        Args:
            state: True or False"""

        # Best effort here
        self._state = state
        self.register()

    def register(self):
        """Registers a supplied node (full path and nodename).

        Returns:
            True: If the nodes registration was updated
            False: If the Zookeeper connection is down

        Raises:
            Exception: If no authorization to update node"""

        # Check if we're in read-only mode
        if not self._zk.connected:
            self.log.warning('Zookeeper connection is down. No command sent.')
            return False

        # Check if we are de-registering a path. If so, give it a best effort
        # brute force attempt
        if self._state is False:
            self.log.warning('Making sure that node is de-registered.')
            try:
                self._zk.retry(self._zk.delete, self._path)
            except:
                pass
            return True

        # Check if this node has already been registered...
        try:
            node = self._zk.get(self._path)
        except kazoo.exceptions.NoNodeError, e:
            pass
        except Exception, e:
            raise e

        # If a node was returned, check whether the data is the exact same or
        # not.
        try:
            if node[0] == self._data:
                self.log.debug('Node already registered and data matches.')
                return True
        except:
            # Any failure is fine here, because if it fails we'll just go and
            # register the node
            pass

        # Register our connection with zookeeper
        try:
            self.log.debug('Attempting to register %s' % self._path)
            self._zk.retry(self._zk.create, self._path, value=self._data,
                           ephemeral=self._ephemeral, makepath=True)
        except kazoo.exceptions.NodeExistsError:
            self.log.debug('Node %s exists, updating data.' % self._path)
            self._zk.retry(self._zk.set, self._path, value=self._data)
        except kazoo.exceptions.NoAuthError, e:
            raise Exception('No authorization to create/set node [%s].' % self._path)

        self.log.debug('Node %s registered with data: %s' %
                      (self._path, self._data))

        return True


class EphemeralNode(Registration):
    """This is a node-specific ephemeral object that we register and monitor.

    The node registered with Zookeeper is ephemeral, so if we lose our
    connection to the service, we have to re-register the data."""

    LOGGING = 'ndServiceRegistry.Registration.EphemeralNode'

    def __init__(self, zk, path, data, state=True):
        """Sets the ZooKeeper registration up to be ephemeral.

        Sets ephemeral=True when we register the Zookeeper node, and
        initiates a simple thread that monitors whether or not our node
        registration has been lost. If it has, it re-registers it."""

        self._ephemeral = True
        Registration.__init__(self, zk, path, data, state=state)

    def stop(self):
        """De-registers from Zookeeper, then calls SuperClass stop() method."""
        # Set our state to disabled to force the de-registration of our node
        self.set_state(False)

        # Call our super class stop()
        return super(EphemeralNode, self).stop()
