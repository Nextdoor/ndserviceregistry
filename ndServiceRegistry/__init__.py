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

"""Simple service registration class for managing lists of servers.

The ServiceRegistry model at Nextdoor is geared around simplicity and
reliability. This model provides a few core features that allow you to
register and unregister nodes that provide certain services, and to monitor
particular service paths for lists of nodes.

Although the service structure is up to you, the ServiceRegistry model is
largely geared towards this model:

  /production
    /ssh
      /server1.cloud.mydomain.com:22
        pid = 12345
      /server2.cloud.mydomain.com:22
        pid = 12345
      /server3.cloud.mydomain.com:22
        pid = 12345
    /web
      /server1.cloud.mydomain.com:80
        pid = 12345
        type = u'apache'

Example usage to provide the above service list:

>>> from ndServiceRegistry import KazooServiceRegistry
>>> nd = KazooServiceRegistry(readonly=False,
                              cachefile='/tmp/cache')
>>> nd.set_node('/production/ssh/server1.cloud.mydomain.com:22')
>>> nd.set_node('/production/ssh/server2.cloud.mydomain.com:22')
>>> nd.set_node('/production/ssh/server3.cloud.mydomain.com:22')
>>> nd.set_node('/production/web/server2.cloud.mydomain.com:22',
                     data={'type': 'apache'})

Example of getting a static list of nodes from /production/ssh. The first time
this get_nodes() function is called, it reaches out to the backend data service
and grabs the data. After that, each time you ask for the same path it is
returned from a local cache object. This local cache object is updated thoug
any time the server list changes, automatically, so you do not need to worry
about it staying up to date.

>>> nd.get_nodes('/production/ssh')
{
  u'server1.cloud.mydomain.com:22': {u'pid': 12345,
                                     u'created': u'2012-12-12 15:26:24'}
  u'server2.cloud.mydomain.com:22': {u'pid': 12345,
                                     u'created': u'2012-12-12 15:26:24'}
  u'server3.cloud.mydomain.com:22': {u'pid': 12345,
                                     u'created': u'2012-12-12 15:26:24'}
}

Example usage to watch '/production/ssh' for servers. In this case, you define
a function that operates on the supplied list of nodes in some way. You then
tell the ServiceRegistry object to notify your function any time the list is
changed in any way.

We do **not** call your function right away. It is up to you whether you want
your callback function executed immediately, or only in the future based on a
change.

>>> def list(nodes):
...     pprint.pprint(nodes)
...
>>> nd.add_callback(list)
>>> list(nd.get_nodes('/production/ssh')
{
  u'server1.cloud.mydomain.com:22': {u'pid': 12345,
                                     u'created': u'2012-12-12 15:26:24'}
  u'server2.cloud.mydomain.com:22': {u'pid': 12345,
                                     u'created': u'2012-12-12 15:26:24'}
  u'server3.cloud.mydomain.com:22': {u'pid': 12345,
                                     u'created': u'2012-12-12 15:26:24'}
}

Copyright 2012 Nextdoor Inc.
"""

__author__ = 'matt@nextdoor.com (Matt Wise)'

# For ServiceRegistry Class
import os
import logging
import exceptions
from os.path import split

# Our own classes
from ndServiceRegistry.registration import EphemeralNode
from ndServiceRegistry.watcher import Watcher
from ndServiceRegistry import funcs
from ndServiceRegistry import exceptions

# For KazooServiceRegistry Class
import kazoo.security
from kazoo.client import KazooClient
from kazoo.client import KazooState

# Use Gevent as our threader (DOES NOT WORK RIGHT NOW)
#from kazoo.handlers.gevent import SequentialGeventHandler as EventHandler
#from gevent.event import Timeout as TimeoutException

# Use standard threading library as our threader
from kazoo.handlers.threading import SequentialThreadingHandler as EventHandler
from kazoo.handlers.threading import TimeoutError as TimeoutException

# Our default variables
from version import __version__ as VERSION

# Defaults
TIMEOUT = 5  # seconds
SERVER = 'localhost:2181'



class ServiceRegistry(object):
    """Main Service Registry object.

    The ServiceRegistry object is a framework object, not meant to be
    instantiated on its own. The object provides a list of functions that an
    object of this type must respond to, and provides the formatting for
    those responses.

    Additionally, common functions are defined in this module when they
    are not unique to a particular service backend (ie, Kazoo vs zc.zk).
    """

    def __init__(self):
        """Initialization of the object."""
        raise NotImplementedError('Not implemented. Use one of my subclasses.')

        #"""Retrieves the data from a particular node from the backend."""
        #raise NotImplementedError('Not implemented')

    def _execute_callbacks(self, path):
        """Runs any callbacks that were passed to us for a given path.

        Args:
            path: A string value of the 'path' that has been updated. This
                  triggers the callbacks registered for that path only."""

        if not path in self._callbacks:
            return

        for callback in self._callbacks[path]:
            callback(self._cache[path])

    def add_callback(self, path, callback):
        """Adds a callback in the event of a path change.

        Adds 'callback' to a dict (self._callbacks) with the path as its
        key. Any time a particular path is updated, we'll walk through the
        list of callbacks under that path key, and run them.

        Args:
            path: A string reprsenting the path to watch for changes.
            callback: Reference to the functino to callback to.
        """

        # Check if the supplied path has ever been requested before. If not
        # then we do not have a 'watch' in place to keep an eye on that path,
        # so adding a local callback wouldn't do much good.
        if not path in self._cache:
            # Path has never been retrieved before, so go get it and establish
            # a watch on that path so that we're notified of any changes.
            self.get_nodes(path)

        if not path in self._callbacks:
            self._callbacks[path] = []

        if not callback in self._callbacks[path]:
                self._callbacks[path].append(callback)

    def get(self, path):
        """Retrieves a list of nodes (or a single node) in dict() form.

        Creates a Watcher object for the supplied path and returns the data
        for the requested path.

        Args:
            path: A string representing the path to the servers.

        Returns:
            dict() of the servers requested, and any data they include
        """

        # Return the object from our cache, if it's there
        self.log.debug('[%s] Checking for existing object...' % path)
        if path in self._watchers:
            self.log.debug('Found [%s] in cache: %s' %
                          (path, str(self._watchers[path].get())))
            return self._watchers[path].get()

        # Ok, so the cache is missing the key. Lets look for it in Zookeeper
        self.log.debug('[%s] Creating Watcher object...' % path)
        self._watchers[path] = Watcher(self._zk, path, watch_children=True)
        return self._watchers[path].get()

    def username(self):
        """Returns self._username"""
        return self._username

    def set_username(self, username):
        """Updates self._username and reconfigures connection.

        Args:
            username: String representing the username"""
        if username == self._username:
            return

        self._username = username
        self.log.debug('Triggering setup_auth')
        self._setup_auth()

    def password(self):
        """Returns self._password"""
        return self._password

    def set_password(self, password):
        """Updates self._password and reconfigures connection.

        Args:
            password: String representing the password"""
        if password == self._password:
            return

        self._password = password
        self.log.debug('Triggering setup_auth')
        self._setup_auth()


class KazooServiceRegistry(ServiceRegistry):

    _instance = None
    _initialized = False
    LOGGER = 'ndServiceRegistry.KazooServiceRegistry'

    def __new__(self, **kwargs):
        """Only creates a new object if one does not already exist."""
        if self._instance is not None:
            return self._instance

        self._instance = object.__new__(self)
        return self._instance

    def __init__(self, server=SERVER, readonly=False, timeout=TIMEOUT,
                 cachefile=None, username=None, password=None,
                 acl=None, lazy=False):
        # See if we're already initialized. If we are, just break out quickly.
        if self._initialized:
            return

        # Create our logger
        self.log = logging.getLogger(self.LOGGER)
        self.log.info('Initializing ServiceRegistry object')

        # Quiet down the Kazoo connection logger no matter what
        self._kz_log = logging.getLogger('kazoo.protocol.connection')
        self._kz_log.setLevel(logging.INFO)

        # Record our supplied settings from the user, in the event that we
        # re-run this init() from the reset() function.
        self._timeout = timeout
        self._username = username
        self._password = password
        self._readonly = readonly
        self._acl = acl
        self._server = server
        self._lazy = lazy
        self._pid = os.getpid()

        # Keep a local dict of all of our DataWatch objects
        self._data_watches = []

        # Create a registrations registry so that we know what paths we've been
        # asked to register. Upon any kind of a reset, we can use this to re-
        # register these paths.
        self._registrations = {}

        # Store all of our owned Watcher objects here
        self._watchers = {}

#        # Create a local 'dict' that we'll use to store the results of our
#        # get_nodes/get_node_data calls.
        self._cache = {}
#        self._cache_file = cachefile

        # Define our zookeeper client here so that it never gets overwritten
        self._zk = KazooClient(hosts=self._server,
                               timeout=self._timeout,
                               read_only=self._readonly,
                               handler=EventHandler(),
                               retry_delay=0.1,
                               retry_backoff=2,
                               retry_max_delay=10)

        # Watch for any connection state changes
        self._zk.add_listener(self._state_listener)

        # Connect (once we're connected, our state listener will handle setting
        # up things like authentication)
        self._connect(lazy)

        # Mark us as initialized
        self._initialized = True
        self.log.info('Initialization Done!')

    def _health_check(func):
        """Decorator used to heathcheck the ZooKeeper connection.

        If this healthcheck fails, we raise a ServiceUnavailable exception.
        If we detect that we've been forked, then we re-create our connection
        to the ZooKeeper backend and move on with our health check."""

        def _health_check_decorator(self, *args, **kwargs):
            self.log.debug('Running healthcheck...')
            pid = os.getpid()
            if pid != self._pid:
                self.log.info('Fork detected!')
                self._pid = pid
                self.reset()

            # check if our connection is up or not
            if not self.CONNECTED:
                return False

            # Nope, we're good
            return func(self, *args, **kwargs)
        return _health_check_decorator

    @_health_check
    def stop(self):
        """Cleanly stops our Zookeeper connection.

        All Registration/Watch objects stick around. If the connection
        is re-established, they will automatically re-create their
        connections."""

        self._zk.stop()

    def start(self):
        """Starts up our ZK connection again.

        All existing watches/registrations/etc will be re-established."""
        self._connect(self._lazy)

    def set(self, node, data, state, type):
        """Registers a supplied node (full path and nodename).

        Registers the supplied node-name with ZooKeeper and converts the
        supplied data into JSON-text.

        Args:
            node: A string representing the node name and service port
                  (/services/foo/host:port)
            data: A dict with any additional data you wish to register.
            state: True/False whether or not the node is actively listed
                   in Zookeeper
            type: A Registration class object representing the type of node
                  we're registering.

        Returns:
            True: registration was sucessfull"""

        # Check if we're in read-only mode
        if self._readonly:
            raise ReadOnlyException('In read-only mode, no writes allowed')

        # Check if the node is already there or not. If it is, we have to
        # figure out if we were the ones who registered it or not. If we are,
        # we leave it alone. If not, we attempt to delete it and register our
        # own. If we cannot do that, we throw an error.
        self.log.debug('Looking for Registration object for [%s] with [%s].'
                       % (node, data))
        if node in self._registrations:
            # The Registration objects can stop themselves in the event of
            # a failure. If they do, lets throw a message, toss the object,
            # and then let a new one be created.
            self.log.debug('[%s] already has Registration object.' % node)
            self._registrations[node].update(data=data, state=state)
            return True

        # Create a new registration object
        self.log.debug('Creating Registration object for [%s]' % node)
        self._registrations[node] = type(zk=self._zk, path=node,
                                         data=data, state=state)
        return True

    def set_node(self, node, data=None, state=True):
        """Registeres an EphemeralNode type."""
        self.set(node=node, data=data, state=state, type=EphemeralNode)

    def unset(self, node):
        """Destroys a particular Registration object.

        Args:
            node: (String) representing the node path to the object"""

        if node in self._registrations:
            self.log.debug('Found Registration object [%s] to delete.' %
                            node)
            self._registrations[node].stop()

    def _connect(self, lazy):
        """Connect to Zookeeper.

        This function starts the connection to Zookeeper using the Kazoo
        start_async() function. By using start_async(), we can continue to
        re-try the connection if it fails either on the initial __init__ of the
        module, or if it fails after the object has been running for a while.

        Args:
            lazy: True/False - determines whether or not we continue to try
                  to connect in the background if the initial connection fails.
        """

        self.log.info('Connecting to Zookeeper Service (%s)' % self._server)

        # Start our connection asynchronously
        self.event = self._zk.start_async()
        self.event.wait(timeout=self._timeout)

        # After the timeout above, check if we're connected.
        if not self._zk.connected:
            # If we allow lazy-connection mode, we'll continue to try to
            # connect in the background. In the foreground, we'll check if
            # theres a locally cached dict() file that we can grab some data
            # from so that we can function partially.
            if lazy:
                self.log.warning(
                    'Could not reach Zookeeper server. '
                    'Starting up in crippled mode. '
                    'Will continue to try to connect in the background.')

                self.log.debug('Loading cache from dict file...')
                if self._cache_file:
                    try:
                        self._cache = funcs.load_dict(self._cache_file)
                    except Exception, e:
                        # If we get an IOError, there's no dict file at all to
                        # pull from, so we start up with an empty dict.
                        self.log.warning(
                            'Could not load up local cache object (%s). '
                            'Starting with no local data. Error: %s' %
                            (self._cache_file, e))
                        pass
            else:
                # If lazy mode is False, then we stop trying to connect to
                # Zookeeper and raise an exception. The client can deal with
                # what-to-do at this point.
                self._zk.stop()
                raise ServiceRegistryException(
                    'Could not connect to ZooKeeper: %s' % e)

    def _setup_auth(self):
        """Set up our credentials with the Zookeeper service.

        If credentials were passwed to us, authenticate with ZooKeeper. These
        credentials do not have to exist in the system, they're compared
        against other credentials to validate whether two users are the same,
        or whether a particular set of credentials has access to a particular
        node.
        """

        if self._username and self._password:
            self.log.debug('Credentials were supplied, creating digest auth.')
            self._zk.retry(self._zk.add_auth, 'digest', "%s:%s" %
                          (self._username, self._password))

            # If an ACL was provided, set it. Otherwise, no ACL is passed and
            # all of our objects are avialable to everybody.
            #
            # This ACL essentially allows our USERNAME+PASSWORD combination to
            # completely own any nodes that were also created with the same
            # USERNAME+PASSWORD combo. This means that if all of your
            # production machines share a particular username/password, they
            # can each mess with the other machines node registrations.
            #
            # Its highly recommended that you  break up your server farms into
            # different permission groups.
            ACL = kazoo.security.make_digest_acl(self._username,
                                                 self._password, all=True)

            # This allows *all* users to read child nodes, but disallows them
            # from reading, updating permissions, deleting child nodes, or
            # writing to child nodes that they do not own.
            READONLY_ACL = kazoo.security.make_acl('world', 'anyone',
                                                   create=False, delete=False,
                                                   write=False, read=True,
                                                   admin=False)

            if not self._acl:
                self._acl = (ACL, READONLY_ACL)

        # If an ACL was providfed, or we dynamically generated one with the
        # username/password, then set it.
        if self._acl:
            self._zk.default_acl = (ACL, READONLY_ACL)

#    @_health_check
#    def _get_node_from_provider(self, node):
#        """Returns the data from the node registered at path"""
#
#        # Check whether we've got an existing DataWatch on this path
#        self._create_datawatch(node)
#
#        # The DataWatch callback only runs after data has been changed, not
#        # on the initial registration. We need to manually fetch the data
#        # once here.
#        directory, nodename = split(node)
#        return self._cache[directory][nodename]

    def _state_listener(self, state):
        """Listens for state changes about our connection.

        If our client becomes disconnected, we set a local variable that lets
        the rest of the code know to not try to run any ZooKeeper commands
        until the service is back up."""

        self.log.warning('Zookeeper connection state changed: %s' % state)
        if state == KazooState.SUSPENDED:
            # In this state, just mark that we can't handle any 'writes' right
            # now but that we might come back to life soon.
            self.CONNECTED = False
        elif state == KazooState.LOST:
            # If we enter the LOST state, we've started a whole new session
            # with the Zookeeper server. Watches are re-established auto-
            # magically. Registered paths are re-established by their own
            # Registration control objects.
            self.CONNECTED = False
        else:
            self.CONNECTED = True
            # We've re-connected, so re-configure our auth digest settings
            self._setup_auth()
