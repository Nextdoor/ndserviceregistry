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

The nd_service_registry model at Nextdoor is geared around simplicity and
reliability. This model provides a few core features that allow you to
register and unregister nodes that provide certain services, and to monitor
particular service paths for lists of nodes.

Although the service structure is up to you, the nd_service_registry model is
largely designed around this model:

  /production
    /ssh
      /server1:22
        pid = 12345
      /server2:22
        pid = 12345
      /server3:22
        pid = 12345
    /web
      /server1:80
        pid = 12345
        type = u'apache'

Example usage to provide the above service list:

    >>> from nd_service_registry import KazooServiceRegistry
    >>> nd = KazooServiceRegistry()
    >>> nd.set_node('/production/ssh/server1:22')
    >>> nd.set_node('/production/ssh/server2:22')
    >>> nd.set_node('/production/ssh/server3:22')
    >>> nd.set_node('/production/web/server2:22',
                    data={'type': 'apache'})

Example of getting a static list of nodes from /production/ssh:

    >>> nd.get('/production/ssh')
    {'children': {u'server1:22': {u'created': u'2012-12-15 01:15:09',
                                  u'pid': 11137},
                  u'server2:22': {u'created': u'2012-12-15 01:15:14',
                                  u'pid': 11137},
                  u'server3:22': {u'created': u'2012-12-15 01:15:18',
                                  u'pid': 11137}},
     'data': None,
     'path': '/production/ssh',
     'stat': ZnodeStat(czxid=27, mzxid=27, ctime=1355533229452,
                       mtime=1355533229452, version=0, cversion=5,
                       aversion=0, ephemeralOwner=0, dataLength=0,
                       numChildren=3, pzxid=45)}

When you call get(), the nd_service_registry module goes out and creates a
Watcher object for the path you provided. This object caches all of the state
data for the supplied path in a local dict. This dict is updated any time the
Zookeeper service sees a change to that path.

Since we're leveraging watches anyways, you can also have your application
notified if a particular path is updated. Its as simple as defining a
function that knows how to handle the above-formatted dict, and passing it
to the get() function.

    >>> def list(nodes):
    ...     import pprint
    ...     pprint.pprint(nodes['children'])
    ...
    >>> nd.get('/production/ssh', callback=list)
    {
      u'server1:22': {u'pid': 12345,
                      u'created': u'2012-12-12 15:26:24'}
      u'server2:22': {u'pid': 12345,
                      u'created': u'2012-12-12 15:26:24'}
      u'server3:22': {u'pid': 12345,
                      u'created': u'2012-12-12 15:26:24'}
    }

Copyright 2012 Nextdoor Inc.
"""

__author__ = 'matt@nextdoor.com (Matt Wise)'

# For nd_service_registry Class
import os
import logging
import exceptions
from os.path import split
from functools import wraps

# Our own classes
from nd_service_registry.registration import EphemeralNode
from nd_service_registry.watcher import Watcher
from nd_service_registry.watcher import DummyWatcher
from nd_service_registry import funcs
from nd_service_registry import exceptions

# For KazooServiceRegistry Class
import kazoo.security
from nd_service_registry.shims import ZookeeperClient
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


class nd_service_registry(object):
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

    def stop(self):
        """Stops our backend connection, and watchers."""
        raise NotImplementedError('Not implemented. Use one of my subclasses.')

    def start(self):
        """Starts our backend connection, re-establishes watchers and regs."""
        raise NotImplementedError('Not implemented. Use one of my subclasses.')

    def set(self, node, data, state, type):
        """Creates a Registration object."""
        raise NotImplementedError('Not implemented. Use one of my subclasses.')

    def set_node(self, node, data=None, state=True):
        """Short-cut for creating an EphemeralNode object."""
        return self.set(node=node, data=data, state=state, type=EphemeralNode)

    def unset(self, node):
        """Disables a Registration object."""
        raise NotImplementedError('Not implemented. Use one of my subclasses.')

    def add_callback(self, path, callback):
        """Registers a function callback for path"""
        raise NotImplementedError('Not implemented. Use one of my subclasses.')

    def get(self, path, callback=None):
        """Retrieves a Watcher.get() dict for a given path."""
        raise NotImplementedError('Not implemented. Use one of my subclasses.')

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

    def _save_watcher_to_dict(self, data):
        """Saves the supplied data to our locally cached dict file.

        This is not meant to be called by hand, but rather by the
        Watcher._execute_callback() function. It takes a dict supplied
        by the get() method from a Watcher object.

        Args:
            data: A dict with the data that we wish to save.
              eg: {'children':
                     {u'server:22': {u'foo': u'bar',
                                     u'created': u'2012-12-17 04:48:46',
                                     u'foo': u'barasdf',
                                     u'more': u'data',
                                     u'pid': 16691}},
                   'data': None,
                   'path': '/services/ssh',
                   'stat': ZnodeStat(czxid=505, mzxid=505, ctime=1355719651687,
                                    mtime=1355719651687, version=0, cversion=1,
                                    aversion=0, ephemeralOwner=0, dataLength=0,
                                    numChildren=1, pzxid=506)}
        """

        if not self._cachefile:
            return

        cache = {data['path']: data}
        self.log.debug('Saving Watcher object to cache: %s' % cache)
        funcs.save_dict(cache, self._cachefile)

    def _get_dummywatcher(self, path, callback=None):
        """Creates DummyWatcher objects from our saved cache.

        Creates a DummyWatcher object for the supplied path and returns the
        object. Triggers callbacks immediately.

        Args:
            path: A string representing the path to the servers.
            callback: (optional) reference to function to call if the path
                      changes.

        Returns:
            DummyWatcher object
        """

        # Initial startup checks
        self.log.debug('[%s] Creating DummyWatcher object...' % path)
        if not self._cachefile:
            self.log.debug('[%s] No cachefile supplied...' % path)
            raise exceptions.ServiceRegistryException(
                'No cachefile supplied, unable to restore item from cache.')

        # Make sure we can load the cache file properly
        self.log.debug('Getting cachefile data...')
        try:
            cache = funcs.load_dict(self._cachefile)
        except (IOError, EOFError), e:
            raise exceptions.ServiceRegistryException(
                'Unable to load cachefile.')

        # Make sure that the path was cached.
        if not path in cache:
            raise exceptions.ServiceRegistryException(
                '[%s] not found in cachefile.' % path)

        # Lastly, try to create an object from the data in the cache.
        try:
            self.log.debug('Creating DummyWatcher object for %s' % path)
            watcher = DummyWatcher(path=path,
                                   data=cache[path],
                                   callback=callback)
            return watcher
        except:
            raise exceptions.ServiceRegistryException(
                'Could not create DummyWatcher object from local cachefile '
                '(%s) for path %s.' % (self._cachefile, path))

    def _convert_dummywatchers_to_watchers(self):
        """Converts all DummyWatcher objects to Watcher objects.

        Walks through our self._watchers dict and attempts to replace each
        DummyWatcher object with a regular Watcher object."""

        for path in self._watchers:
            if isinstance(self._watchers[path], DummyWatcher):
                self.log.debug('Found DummyWatcher for %s' % path)
                w = None
                try:
                    w = self._get_watcher(path)
                except Exception, e:
                    self.log.warning('Could not create Watcher '
                                     'object for %s: %s' % (path, e))
                if w:
                    # Get a list of all callbacks associated with
                    # the DummyWatcher
                    for callback in self._watchers[path]._callbacks:
                        w.add_callback(callback)

                    self._watchers[path] = w


class KazooServiceRegistry(nd_service_registry):

    _instance = None
    _initialized = False
    LOGGER = 'nd_service_registry.KazooServiceRegistry'

    def __new__(self, **kwargs):
        """Only creates a new object if one does not already exist."""
        if self._instance is not None:
            return self._instance

        self._instance = object.__new__(self)
        return self._instance

    def __init__(self, server=SERVER, readonly=False, timeout=TIMEOUT,
                 cachefile=None, username=None, password=None,
                 acl=None, lazy=False):
        """Initialize the KazooServiceRegistry object.

        Generally speaking, you can initialize the object with no explicit
        settings and it will work fine as long as your Zookeeper server is
        available at 'localhost:2181'.

        A few notes though about some of the options...

        'lazy' mode allows the object to initialize and lazily connect to
               the Zookeeper services. If Zookeeper is down, it will continue
               to try to connect in the background, while allowing the object
               to respond to certain queries out of the 'cachefile'. This
               option is not very useful without the 'cachefile' setting.

       'cachefile' is a location to save a pickle-dump of our various
                   data objects. In the event that we start up in 'lazy' mode
                   and are unable to reach the backend Zookeeper service,
                   objects are re-created with this cache file instead and
                   we are able to respond to get() requests with the latest
                   saved data.

                   Of important note here, if you have multiple processes
                   using the same file location, they will not overwrite, but
                   rather they will append to the file as they save data. Each
                   object saves to this file immediately upon receiving a
                   data-change from the objects... so its generally very up-
                   to-date.

        'username' and 'password's can be supplied if you wish to authenticate
                   with Zookeeper by creating a 'Digest Auth'.

        'readonly' mode just safely sets our connection status to readonly to
                   prevent any accidental data changes. Good idea for your
                   data consumers.

        Args:
            server: String in the format of 'localhost:2181'
            readonly: Boolean that sets whether our connection to Zookeeper
                      is a read_only connection or not.
            timeout: Value in seconds for connection timeout
            lazy: Boolean whether or not to allow lazy connection mode
            cachefile: Location of desired cachefile
            username: Username to create Digest Auth with
            password: Password to create Digest Auth with
            acl: A Kazoo ACL-object if special ACLs are desired
        """

        # See if we're already initialized. If we are, just break out quickly.
        if self._initialized:
            return

        # Create our logger
        self.log = logging.getLogger(self.LOGGER)
        self.log.info('Initializing ServiceRegistry object')

        # Quiet down the Kazoo connection logger no matter what
        logging.getLogger('kazoo.protocol.connection').setLevel(logging.INFO)

        # Record the supplied settings
        self._timeout = timeout
        self._username = username
        self._password = password
        self._readonly = readonly
        self._acl = acl
        self._server = server
        self._lazy = lazy
        self._pid = os.getpid()

        # Store all of our Registration objects here
        self._registrations = {}

        # Store all of our Watcher objects here
        self._watchers = {}

        # Create a local 'dict' that we'll use to store the results of our
        # get_nodes/get_node_data calls.
        self._cachefile = cachefile

        # Define our zookeeper client here so that it never gets overwritten
        self._zk = ZookeeperClient(hosts=self._server,
                                   timeout=self._timeout,
                                   read_only=self._readonly,
                                   handler=EventHandler(),
                                   retry_delay=0.1,
                                   retry_backoff=2,
                                   retry_max_delay=10)

        # Get a lock handler
        self._run_lock = self._zk.handler.lock_object()

        # Watch for any connection state changes
        self._zk.add_listener(self._state_listener)

        # Connect (once we're connected, our state listener will handle setting
        # up things like authentication)
        self._connect(lazy)

        # Mark us as initialized
        self._initialized = True
        self.log.info('Initialization Done!')

    def _health_check(func):
        """Decorator used to heathcheck the Zookeeper connection.

        If this healthcheck fails, we raise a ServiceUnavailable exception.
        If we detect that we've been forked, then we re-create our connection
        to the Zookeeper backend and move on with our health check."""

        @wraps(func)
        def _health_check_decorator(self, *args, **kwargs):
            self.log.debug('Running healthcheck...')
            pid = os.getpid()
            if pid != self._pid:
                self.log.warning('Fork detected!')
                self._pid = pid
                try:
                    self.stop()
                except:
                    # Allow this to fail.. we'll just restart the conn anyways
                    pass
                self._connect(lazy=False)

            # check if our connection is up or not
            if not self._zk.connected:
                e = 'Service is down. Try again later.'
                raise exceptions.NoConnection(e)

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

    @_health_check
    def set(self, node, data, state, type):
        """Registers a supplied node (full path and nodename).

        Registers the supplied node-name with Zookeeper and converts the
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
            True: registration was successful"""

        # Check if we're in read-only mode
        if self._readonly:
            raise exceptions.ReadOnly('In read-only mode, no writes allowed')

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

    def unset(self, node):
        """Destroys a particular Registration object.

        Args:
            node: (String) representing the node path to the object"""

        try:
            self._registrations[node].stop()
        except:
            self.log.warning('Node object for %s not found.' % node)
            return

        self.log.info('Registration for %s stopped.' % node)

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
            else:
                # If lazy mode is False, then we stop trying to connect to
                # Zookeeper and raise an exception. The client can deal with
                # what-to-do at this point.
                self._zk.stop()
                raise exceptions.NoConnection(
                    'Could not connect to Zookeeper')

    @_health_check
    def _setup_auth(self):
        """Set up our credentials with the Zookeeper service.

        If credentials were passwed to us, authenticate with Zookeeper. These
        credentials do not have to exist in the system, they're compared
        against other credentials to validate whether two users are the same,
        or whether a particular set of credentials has access to a particular
        node.
        """

        with self._run_lock:
            if self._username and self._password:
                # If an ACL was provided, we'll use it. If not though, we'll
                # create our own ACL described below:
                #
                # This ACL essentially allows our USERNAME+PASSWORD combo to
                # completely own any nodes that were also created with the same
                # USERNAME+PASSWORD combo. This means that if all of your
                # production machines share a particular username/password,
                # they can each mess with the other machines node
                # registrations.
                #
                # Its highly recommended that you break up your server farms
                # into different permission groups.
                ACL = kazoo.security.make_digest_acl(self._username,
                                                     self._password, all=True)

                # This allows *all* users to read child nodes, but disallows
                # them from reading, updating permissions, deleting child
                # nodes, or writing to child nodes that they do not own.
                READONLY_ACL = kazoo.security.make_acl('world', 'anyone',
                                                       create=False,
                                                       delete=False,
                                                       write=False,
                                                       read=True,
                                                       admin=False)

                self.log.debug('Credentials were supplied, adding auth.')
                self._zk.retry(self._zk.add_auth, 'digest', "%s:%s" %
                              (self._username, self._password))

                if not self._acl:
                    self._acl = (ACL, READONLY_ACL)

            # If an ACL was providfed, or we dynamically generated one with the
            # username/password, then set it.
            if self._acl:
                self._zk.default_acl = (ACL, READONLY_ACL)

    def _state_listener(self, state):
        """Listens for state changes about our connection.

        If our client becomes disconnected, we set a local variable that lets
        the rest of the code know to not try to run any Zookeeper commands
        until the service is back up."""

        self.log.warning('Zookeeper connection state changed: %s' % state)
        if state == KazooState.SUSPENDED:
            # In this state, just mark that we can't handle any 'writes' right
            # now but that we might come back to life soon.
            return
        elif state == KazooState.LOST:
            # If we enter the LOST state, we've started a whole new session
            # with the Zookeeper server. Watches are re-established auto-
            # magically. Registered paths are re-established by their own
            # Registration control objects.
            return
        else:
            # We've re-connected, so re-configure our auth digest settings
            self._setup_auth()

            # We are not allowed to call any blocking calls in this callback
            # because it is actually triggered by the thread that holds onto
            # the Zookeeper connection -- and that causes a deadlock.
            #
            # In order to re-register our Watchers, we use the Kazoo spawn()
            # function. This runs the function in a separate thread and allows
            # the state_listener function to return quickly.
            self._zk.handler.spawn(self._convert_dummywatchers_to_watchers)

    def add_callback(self, path, callback):
        """Adds a callback in the event of a path change.

        Adds a callback to a given watcher. If the watcher doesnt exist,
        we create it with that callback. Either way, your callback is
        immediately executed with the service data.

        Args:
            path: A string reprsenting the path to watch for changes.
            callback: Reference to the function to callback to.
        """

        if path in self._watchers:
            self.log.debug('Found [%s] in watchers. Adding callback.' % path)
            self._watchers[path].add_callback(callback)
        else:
            self.log.debug('No existing watcher for [%s] exists. Creating.' %
                           path)
            self.get(path, callback=callback)

    def get(self, path, callback=None):
        """Retrieves a list of nodes (or a single node) in dict() form.

        Creates a Watcher object for the supplied path and returns the data
        for the requested path. Triggers callback immediately.

        Args:
            path: A string representing the path to the servers.
            callback: (optional) reference to function to call if the path
                      changes.

        Returns:
            nd_service_registry.Watcher.get() dict object
        """

        # Return the object from our cache, if it's there
        self.log.debug('[%s] Checking for existing object...' % path)
        if path in self._watchers:
            self.log.debug('Found [%s] in cache: %s' %
                          (path, str(self._watchers[path].get())))
            # If a callback was suplied, but we already have a Watcher object,
            # add that callback to the existing object.
            if callback:
                self._watchers[path].add_callback(callback)

            # Return the Watcher object get() data.
            return self._watchers[path].get()

        try:
            # Go get a Watcher object since one doesnt already exist
            self._watchers[path] = self._get_watcher(path, callback)
            return self._watchers[path].get()
        except exceptions.NoConnection, e:
            # Get a DummyWatcher cached object instead
            self.log.warning('Health Check failed: %s' % e)

        try:
            self.log.info('[%s] Loading from cache instead' % path)
            self._watchers[path] = self._get_dummywatcher(path, callback)
            return self._watchers[path].get()
        except exceptions.ServiceRegistryException, e:
            # Ugh. Total failure. Return false
            self.log.error('Unable to retrieve [%s] from Zookeeper or cache - '
                           'try again later: %s' % (path, e))
        return False

    @_health_check
    def _get_watcher(self, path, callback=None):
        """Creates a Watcher object for the supplid path.

        Creates a Watcher object for the supplied path and returns the
        object. Triggers callbacks immediately.

        This is broken into its own function so that it can leverage
        the @_health_check decorator. This function should never be
        called from outside of this class.

        Args:
            path: A string representing the path to the servers.
            callback: (optional) reference to function to call if the path
                      changes.

        Returns:
            nd_service_registry.Watcher object
        """

        # Ok, so the cache is missing the key. Lets look for it in Zookeeper
        self.log.debug('[%s] Creating Watcher object...' % path)
        watcher = Watcher(self._zk,
                          path,
                          watch_children=True,
                          callback=callback)

        # Always register our dictionary saver callback, in addition to
        # whatever our user has supplied
        watcher.add_callback(self._save_watcher_to_dict)
        return watcher
