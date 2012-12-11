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

Example usage:

from ndServiceRegistry import KazooServiceRegistry
nd = KazooServiceRegistry(server='localhost:2182', readonly=False,
                          cachefile='/tmp/cache', username='test',
                          password='test')
nd.get_nodes('/services/ssh')
nd.register_node('/services/ssh/FOOBAR:123', data={'foo': 'bar'})

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
from ndServiceRegistry import funcs

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


class ServiceRegistryException(Exception):

    """ServiceParty Exception Object"""

    def __init__(self, e):
        self.value = e

    def __str__(self):
        return self.value


class NoAuthException(ServiceRegistryException):
    """Thrown when we have no authorization to perform an action."""


class BackendUnavailableException(ServiceRegistryException):
    """Thrown when the backend is unavilable for some reason."""


class ReadOnlyException(ServiceRegistryException):
    """Thrown when a Write operation is attempted while in Read Only mode."""


class ServiceRegistry(object):
    """Main Service Registry object.

    This object provides answers to the question:
      'What servers offer XYZ service?'

    We use a Singleton object within this class to make sure that
    we only open up a single connectino to our data backend. Additionally
    the data is cached locally and only updated when the data changes,
    so having the object re-created would waste this cache.
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
            callback: Reference to the method to callback to.
        """

        if not path in self._callbacks:
            self._callbacks[path] = []

        if not callback in self._callbacks[path]:
                self._callbacks[path].append(callback)

    def get_nodes(self, path):
        """Retrieve a list (dict) of servers for a given path.

        Returns a list of servers (host:port) format with any applicable
        config data (usernames, passwords, etc). Data is returned in a
        dict, and cached. Once the dict exists, data is always returned
        from the dict first.

        Args:
            path: A string representing the path to the servers.

        Returns:
            dict() of the servers requested, and any data they include
        """

        # Return the object from our cache, if it's there
        self.log.debug('Checking for [%s] in cache.' % path)
        if path in self._cache:
            self.log.debug('Found [%s] in cache. Nodes: %s' %
                          (path, str(self._cache[path])))
            return self._cache[path]

        # Ok, so the cache is missing the key. Lets look for it in Zookeeper
        self.log.debug('Checking for [%s] in data provider.' % path)
        return self._get_nodes_from_provider(path)

    def get_node(self, node):
        """Retrieve a dict of data for the node supplied.

        Returns the data from a given path (if it is a node). If it does
        not exist, returns None.

        Args:
            path: A string representing the path to the server.
            node: A string representing the actual node identifier

        Returns:
            dict() of the servers requested, and any data they include
            None if no data is available
            False if the server does not exist at all at that path
        """

        # Return the object from our cache, if it's there. If we have the path
        # in our local cache, assume its authoritative and do not bother asking
        # backend at all.
        directory, nodename = split(node)
        self.log.debug('Checking for [%s] in cache.' % node)
        if directory in self._cache:
            if nodename in self._cache[directory]:
                self.log.debug('Node %s found in cache. Data: %s' %
                              (node, str(self._cache[directory][nodename])))
                return self._cache[directory][nodename]
            else:
                self.log.debug('Node %s not found in cache.' % node)
                return False

        # Ok, so the cache is missing the key. Lets look for it in the backend
        self.log.debug('Checking for [%s] in data provider.' % node)
        return self._get_node_from_provider(node)


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
        self.log.setLevel(logging.DEBUG)
        self.log.info('Initializing ServiceRegistry object')

        # Quiet down the Kazoo connection logger no matter what
        self._kz_log = logging.getLogger('kazoo.protocol.connection')
        self._kz_log.setLevel(logging.INFO)

        # Record our supplied settings from the user, in the event that we
        # re-run this init() from the reset() method.
        self._timeout = timeout
        self._username = username
        self._password = password
        self._readonly = readonly
        self._acl = acl
        self._server = server
        self._pid = os.getpid()

        # Create a callback registry so that other objects can be notified
        # when our server lists change.
        self._callbacks = {}

        # Create a registrations registry so that we know what paths we've been
        # asked to register. Upon any kind of a reset, we can use this to re-
        # register these paths.
        self._registrations = {}

        # Create a local 'dict' that we'll use to store the results of our
        # get_nodes/get_node_data calls.
        self._cache = {}
        self._cache_file = cachefile

        # Connect (once we're connected, our state listener will handle setting
        # up things like authentication)
        self._connect(lazy)

        # Mark us as initialized
        self._initialized = True
        self.log.info('Initialization Done!')

    def register_node(self, node, data=None, state=True):
        """Registers a supplied node (full path and nodename).

        Registers the supplied node-name with ZooKeeper and converts the
        supplied data into JSON-text.

        Args:
            node: A string representing the node name and service port
                  (/services/foo/host:port)
            data: A dict with any additional data you wish to register.
            state: True/False whether or not the node is actively listed
                   in Zookeeper

        Returns:
            True: registration was sucessfull"""

        # Check if we're in read-only mode
        if self._readonly:
            raise ReadOnlyException('In read-only mode, this operation cannot '
                                    'be completed.')

        # Check if the node is already there or not. If it is, we have to
        # figure out if we were the ones who registered it or not. If we are,
        # we leave it alone. If not, we attempt to delete it and register our
        # own. If we cannot do that, we throw an error.
        self.log.debug('Registering [%s] with [%s].' % (node, data))
        if node in self._registrations:
            self.log.debug('[%s] already has Registration object.' % node)
            self._registrations[node].set_state(state)
            return self._registrations[node]

        # Create a new registration object
        self.log.info('Creating Registration object for [%s]' % node)
        self._registrations[node] = EphemeralNode(zk=self._zk, path=node,
                                                  data=data, state=state)

    def _connect(self, lazy):
        """Connect to Zookeeper.

        This method starts the connection to Zookeeper using the Kazoo
        start_async() method. By using start_async(), we can continue to re-try
        the connection if it fails either on the initial __init__ of the
        module, or if it fails after the object has been running for a while.

        Args:
            lazy: True/False - determines whether or not we continue to try
                  to connect in the background if the initial connection fails.
        """

        self.log.info('Connecting to Zookeeper Service (%s)' % self._server)
        self._zk = KazooClient(hosts=self._server,
                               timeout=self._timeout,
                               read_only=self._readonly,
                               handler=EventHandler(),
                               retry_delay=0.1,
                               retry_backoff=2,
                               retry_max_delay=10)

        # Watch for any connection state changes
        self._zk.add_listener(self._state_listener)

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
                        # If we get an IOError, there's no dict file at all to pull
                        # from, so we start up with an empty dict.
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
    def _get_nodes_from_provider(self, path):
        """Returns a list of children from Zookeeper.

        Go to Zookeeper and get a list of hosts from a particular path.
        Raises an exception if we cannot return a valid result.

        Args:
            path: A string representing the path to check in Zookeeper.

        Returns:
            <dict> object of server list in hostname:port form
        """

        # Get an iterator for the path
        self.log.debug('Registering a kazoo.ChildrenWatch object for '
                       'path [%s].' % path)

        # Create a function that updates our local cache
        @self._zk.ChildrenWatch(path)
        def update(nodes):
            self._cache[path] = dict()
            for node in sorted(nodes):
                self._cache[path][node] = self._get_node_from_provider(
                    str(path + '/' + node))
            self.log.info('%s path has updated nodes: %s' %
                         (path, str(self._cache[path])))
            if self._cache_file:
                funcs.save_dict(self._cache, self._cache_file)
            self._execute_callbacks(path)
        return self._cache[path]

    @_health_check
    def _get_node_from_provider(self, node):
        """Returns the data from the node registered at path"""
        try:
            d = self._zk.retry(self._zk.get, node)
            return funcs.decode(d[0])
        except:
            # If this fails, the node likely does not exist.
            # Return False in this case.
            return False

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
