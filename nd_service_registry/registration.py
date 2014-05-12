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

"""Kazoo Zookeeper znode registration client

This object type handles the initial registration, updating of registered
data, and connection state changes from the supplied ServiceRegistry object.

The idea here is that the Registration object creates a Watcher object to
keep an eye on the 'node' that we want to register. This Watcher object
will then trigger the this Registration object to create/update or delete
the node, based on the desired state (self.state() and self.set_state()).

Args:
    zk: kazoo.client.KazooClient object reference
    path: (string) The full path to register (including hostname,
          if applicable)
    data: (dict/string) Data to apply to the supplied path
    state: (Boolean) whether to create, or delete the path from ZooKeeper
    ephemeral: (Boolean) whether to create an ephemeral node, default: False

Example:
    Register a new Ephemeral node:
    >>> r = EphemeralNode(zk, '/services/ssh/foo:123', 'my data', True)
    >>> r.data()
    {u'pid': 8364, u'string_value': u'my data',
     u'created': u'2012-12-14 21:17:50'}

    Now change the nodes data
    >>> r.set_data('some other data')
    >>> r.data()
    {u'pid': 8364, u'string_value': u'some other data',
     u'created': u'2012-12-14 21:18:26'}

    De-register the node
    >>> r.set_state(False)
    >>> r.data()
    >>> r.get()
    {'stat': None, 'data': None, 'children': {}}

Copyright 2014 Nextdoor Inc."""

__author__ = 'matt@nextdoor.com (Matt Wise)'

import logging

from nd_service_registry import funcs
from nd_service_registry.watcher import Watcher

# For KazooServiceRegistry Class
import kazoo.exceptions

TIMEOUT = 30

log = logging.getLogger(__name__)


class RegistrationBase(object):
    """Base object model for registering data in Zookeeper.

    This object is not meant to be used directly -- its not complete, and
    will not behave properly. It is simply a shell that other data types can
    subclass."""

    GENERAL_EXC_MSG = ('[%s] Received exception. Moving on.'
                       ' Will not re-attempt this command: %s')

    def __init__(self, zk, path, data=None, state=False, ephemeral=False):
        # Set our local variables
        self._ephemeral = ephemeral
        self._zk = zk
        self._path = path
        self._state = state

        # Store both encdoed-string and decoded-dict versions of our 'data'
        # for comparison purposes later.
        self._data = data
        self._encoded_data = funcs.encode(data)
        self._decoded_data = funcs.decode(self._encoded_data)

        # Set a default watcher without a callback.
        self._watcher = Watcher(self._zk,
                                path=self._path,
                                watch_children=False)

    def data(self):
        """Returns live node data from Watcher object."""
        return self._watcher.get()['data']

    def get(self):
        """Returns live node information from Watcher object."""
        return self._watcher.get()

    def set_data(self, data):
        """Sets self._data.

        Args:
            data: String or Dict of data to register with this object."""
        if not data == self._data:
            self._data = data
            self._encoded_data = funcs.encode(data)
            self._decoded_data = funcs.decode(self._encoded_data)
            self._update_data()

    def stop(self):
        """Disables our registration of the node."""
        self.set_state(False)

    def start(self):
        """Enables our registration of the node."""
        self.set_state(True)

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

        if self._state == state:
            return

        self._state = state
        self._update_state(self._state)

    def update(self, data=None, state=None):
        """If data or state are supplied, these are updated before triggering
        the update.

        Args:
            data: (String/Dict) data to register with this object.
            state: (Boolean) whether to register or unregister this object
        """
        if type(state) is bool:
            log.debug('[%s] Got updated state: %s' % (self._path, state))
            self.set_state(state)

        if data:
            log.debug('[%s] Got updated data: %s' % (self._path, data))
            self.set_data(data)

    def _update_state(self, state):
        if state is True:
            # Register our connection with zookeeper
            try:
                log.debug('[%s] Registering...' % self._path)
                self._zk.retry(self._zk.create, self._path,
                               value=self._encoded_data,
                               ephemeral=self._ephemeral, makepath=True)
                log.info('[%s] Registered with data: %s' %
                         (self._path, self._encoded_data))
            except kazoo.exceptions.NodeExistsError, e:
                # Node exists ... possible this callback got called multiple
                # times
                pass
            except kazoo.exceptions.NoAuthError, e:
                log.error('[%s] No authorization to create node.' % self._path)
            except Exception, e:
                log.error(RegistrationBase.GENERAL_EXC_MSG % (
                    self._path, e))

        elif state is False:
            # Try to delete the node
            log.debug('[%s] Attempting de-registration...' % self._path)
            try:
                self._zk.retry(self._zk.delete, self._path)
            except kazoo.exceptions.NoAuthError, e:
                # The node exists, but we don't even have authorization to read
                # it. We certainly will not have access then to change it below
                # so return false. We'll retry again very soon.
                log.error('[%s] No authorization to delete node.' % self._path)
            except Exception, e:
                log.error(RegistrationBase.GENERAL_EXC_MSG % (
                    self._path, e))

    def _update_data(self):
        try:
            self._zk.retry(self._zk.set, self._path, value=self._encoded_data)
            log.debug('[%s] Updated with data: %s' %
                      (self._path, self._encoded_data))
        except kazoo.exceptions.NoAuthError, e:
            log.error('[%s] No authorization to set node.' % self._path)
        except Exception, e:
            log.error(RegistrationBase.GENERAL_EXC_MSG % (
                      self._path, e))


class EphemeralNode(RegistrationBase):
    """This is a node-specific ephemeral object that we register and monitor.

    The node registered with Zookeeper is ephemeral, so if we lose our
    connection to the service, we have to re-register the data."""

    GENERAL_EXC_MSG = ('[%s] Received exception. Moving on, will re-attempt '
                       'when Watcher notifies us of a state change: %s')

    def __init__(self, zk, path, data=None, state=True):
        """Sets the ZooKeeper registration up to be ephemeral.

        Sets ephemeral=True when we register the Zookeeper node, and
        initiates a simple thread that monitors whether or not our node
        registration has been lost. If it has, it re-registers it."""

        RegistrationBase.__init__(self, zk, path, data,
                                  state=state, ephemeral=True)
        self._watcher.add_callback(self._update)

    def _update(self, data):
        """Registers a supplied node (full path and nodename)."""

        # Try to delete the node
        log.debug('[%s] Called with data: %s' % (self._path, data))
        log.debug('[%s] Wanted state: %s' % (self._path, self.state()))

        if self.state() is False and data['stat'] is not None:
            # THe node exists because data['stat'] has data, but our
            # desired state is False. De-register the node.
            self._update_state(False)
        elif self.state() is True and data['stat'] is None:
            # The node does NOT exist because data['stat'] is None,
            # but our desired state is True. Register the node.
            self._update_state(True)
            return
        elif self.state() is True and not data['data'] == self._decoded_data:
            # Lastly, the node is registered, and we want it to be. However,
            # the data with the node is incorrect. Change it.
            log.warning('[%s] Registered node had different data.' %
                        self._path)
            self._update_data()

    def stop(self):
        """De-registers from Zookeeper, then calls SuperClass stop() method."""
        # Set our state to disabled to force the de-registration of our node
        self.set_state(False)

        # Call our super class stop()
        return super(EphemeralNode, self).stop()


class DataNode(RegistrationBase):
    """This is an registry object that we register arbitrary data and monitor.

    The node registered with Zookeeper is not ephemeral. If data is changed
    in Zookeeper, this node is updated via the Watcher object."""

    def __init__(self, zk, path, data=None, state=True):
        RegistrationBase.__init__(self, zk, path, data,
                                  state=state, ephemeral=False)

        # Regardless of what the state of the node in Zookeeper is,
        # explicitly set it when this DataNode is instantiated.
        self._update_state(state)
        self._update_data()

        # Now, set a callback so that if the Watcher detects a remote
        # data change, our local self._data object is updated.
        self._watcher.add_callback(self._update)

    def _update(self, data):
        """Updates the DataNode objects data values.

        If the values change via the Watcher object, update the DataNode cached
        settings so that we know what the current state of Zookeeper is.

        These are updated with the true values in Zookeeper (regardless of what
        the initial object was created with) so that when the set_data()
        method is called, we only make updates to Zookeeper if necessary."""

        log.debug('[%s] Received updated data: %s' % (self._path, data))

        # If the returned 'stat' is empty, then the node was deleted in
        # Zookeeper and we should update our local 'state'.
        if not data['stat']:
            log.debug("[%s] No stat supplied, setting local state "
                      "to false." % self._path)
            self._state = False

        # Quickly check that if data['data'] is none, we just clear our
        # settings and jump out of this method.
        if not data['data']:
            log.error("[%s] No data supplied at all. Wiping out "
                      "local data cache." % self._path)
            self._encoded_data = funcs.encode(None)
            self._decoded_data = None
            self._data = None
            return

        # First, store the directly supplied data as our _decoded_data
        # (string), and then decode that into a proper hash and store it
        # as our _encoded_data.
        self._encoded_data = funcs.encode(data['data'])
        self._decoded_data = dict(data['data'])

        # Strictly speaking, the self._data object should contain the user
        # supplied data object, without any of the additional data
        # automatically supplied by funcs.default_data(). If this DataNode
        # object receives an updated bunch of data from Zookeeper, it will
        # include these additional data parameters. We need to strip those
        # out first, to ensure that we're only storing the user-supplied
        # parameters.
        self._data = dict(data['data'])
        for k in funcs.default_data().keys():
            self._data.pop(k, None)

        # If the only key left in the self._data object is 'string_value',
        # then the supplied user data was actually in string format -- so
        # thats actually what we want to save.
        if self._data.keys() == ['string_value']:
            self._data = self._data['string_value']
