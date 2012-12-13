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

    In order to constantly maintain our state with the Zookeeper service we
    run this as a threading.Thread object with a run() function loop that
    checks every 30 seconds whether or not the host is still registered.
    If the node has changed for any reason (connection loss, some delete
    event, etc), we attempt to re-create the node reference and update its
    data.

    In addition to this 30 second loop, calling self.update() will trigger
    an update of the data within just a few seconds.

    Args:
        zk: kazoo.client.KazooClient object reference
        path: (string) The full path to register (including hostname,
              if applicable)
        data: (dict/string) Data to apply to the supplied path
        state: (Boolean) whether to create, or delete the path from ZooKeeper
    """

    LOGGING = 'ndServiceRegistry.Registration'

    def __init__(self, zk, path, data, state=True):
        # Initiate our thread
        super(Registration, self).__init__()

        # Create our logger
        self.log = logging.getLogger(self.LOGGING)

        # Set our local variables
        self._zk = zk
        self._path = path
        self._state = state
        self._registered = False
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
        self._registered = False

    def path(self):
        """Returns self._path"""
        return self._path

    def run(self):
        """Simple background thread that monitors our Zookeeper registration.

        We want our threads to respond to an outside Exception extremely
        quickly, however, we don't want to run our self._update() loop super
        often. What we do here is loop every half-second and check whether the
        last-time the self._update() method executed is greater than TIMEOUT.
        """

        last_run = 0
        while True and not self._event.is_set():
            self._event.wait(1)
            if time.time() - last_run >= TIMEOUT or self._registered == False:
                self.log.debug('Executing self._update()')
                self._registered = self._update()
                last_run = time.time()

        # Let go of our _zk reference
        self._zk = None
        self.log.debug('Registration %s is exiting run() loop.' % self._path)

    def stop(self):
        """Stops the run() method."""
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
        self._registered = False

    def update(self):
        """Triggers near-immediate run of the self._update() function"""
        self._registered = False

    def _update(self):
        """Registers a supplied node (full path and nodename).

        Returns:
            True: If the nodes registration was updated
            False: If the Zookeeper connection is down

        Raises:
            NoAuthException: If no authorization to update node"""

        # Check if we're in read-only mode
        if not self._zk.connected:
            self.log.warning('Zookeeper connection is down. Will retry later.')
            return False

        # Check if we are de-registering a path. If so, give it a best effort
        # brute force attempt
        if self._state is False:
            self.log.debug('Making sure that node is de-registered.')
            if self._zk.exists(self._path):
                try:
                    self._zk.retry(self._zk.delete, self._path)
                except Exception, e:
                    # Do our absolute best to remove the node, but if the
                    # command fails, we'll throw a log message and move on.
                    self.log.warn('Could not remove %s from Zookeeper: %s' %
                                  (self._path, e))     
                    # Return False because the ndoe existed, but we were unable
                    # to delete its registration. This will trigger a retry
                    # very quickly because of the loop in run().
                    return False
            # We suceeded at de-registering our node, so return true.
            return True

        # Check if this node has already been registered...
        try:
            if self._zk.exists(self._path):
                node = self._zk.get(self._path)
        except kazoo.exceptions.NoNodeError, e:
            # The node isn't registered yet, this is fine
            pass
        except kazoo.exceptions.NoAuthError, e:
            # The node exists, but we don't even have authorization to read
            # it. We certainly will not have access then to change it below,
            # so return false. We'll retry again very soon.
            self.log.error(('No authorization to delete node [%s]. '
                            'Will retry on next loop.' % self._path))
            return False
        except:
            # Raise any completely unknown exceptions.
            self.log.error('Stopping run loop, unexpected exception raised: %s'
                           % sys.exc_info()[0])
            self.stop()
            return False

        # If a node was returned, check whether the data is the exact same or
        # not.
        try:
            if node[0] == self._data:
                self.log.debug('Node already registered and data matches.')
                return True
        except:
            # We must not have retrieved a node above, so just ignore this and
            # move on with the registration below.
            pass

        # Register our connection with zookeeper
        try:
            self.log.debug('Attempting to register %s' % self._path)
            self._zk.retry(self._zk.create, self._path, value=self._data,
                           ephemeral=self._ephemeral, makepath=True)
            self.log.debug('Node %s registered with data: %s' %
                          (self._path, self._data))
        except kazoo.exceptions.NodeExistsError:
            self.log.debug('Node %s exists, updating data.' % self._path)
            self._zk.retry(self._zk.set, self._path, value=self._data)
            self.log.debug('Node %s updated with data: %s' %
                          (self._path, self._data))
        except kazoo.exceptions.NoAuthError, e:
            self.log.error(('No authorization to create/set node [%s]. '
                            'Will retry on next loop.' % self._path))
            return False
        except:
            # Raise any completely unknown exceptions.
            self.log.error('Stopping run loop, unexpected exception raised: %s'
                           % sys.exc_info()[0])
            self.stop()
            return False

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
