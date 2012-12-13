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
>>> nd.set('/production/ssh/server1.cloud.mydomain.com:22')
>>> nd.set('/production/ssh/server2.cloud.mydomain.com:22')
>>> nd.set('/production/ssh/server3.cloud.mydomain.com:22')
>>> nd.set('/production/web/server2.cloud.mydomain.com:22',
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
        """Walks through our object and stops everything.

        Terminates our ZK connection gracefully."""
        self._zk.stop()

    def start(self):
        """Starts up our ZK connection again.

        All existing watches/registrations/etc will be re-established."""
        self._connect(self._lazy)

    def set(self, node, data=None, state=None):
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
            if self._registrations[node].is_alive():
                self._registrations[node].update(data=data, state=state)
                return True
            else:
                self.log.debug('[%s] Registration object is dead.' % node)
                del self._registrations[node]

        # Create a new registration object
        self.log.debug('Creating Registration object for [%s]' % node)
        self._registrations[node] = EphemeralNode(zk=self._zk, path=node,
                                                  data=data, state=state)
        return True

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

        # Establish a watch on the entire path for any updates
        @self._zk.ChildrenWatch(path)
        def _update_node_list(nodes):
            self.log.info('%s path node list has changed.' % path)
            # Create a temporary local dict and fill it with all of our
            # new node data
            temp_cache = dict()
            for node in sorted(nodes):
                # Get the data and the full path name
                fullpath = str(path + '/' + node)
                temp_cache[node] = funcs.decode(
                    self._zk.retry(self._zk.get, fullpath)[0])

                # Now, call _create_data_watch to create a new DataWatch
                # object for this path.
                self._create_datawatch(fullpath)

            # Swap in our new dict data to the local self._cache object
            self._cache[path] = temp_cache

            # Now we have a full new node list
            self.log.info('%s path new node list: %s' %
                         (path, self._cache[path]))

            # If we are saving our caches to a local file, do it
            if self._cache_file:
                funcs.save_dict(self._cache, self._cache_file)

            # Execute any callbacks
            self._execute_callbacks(path)

        # Establish a watch on each individual server listed in the
        # supplied path. Any time that the parent update() function is
        # called, which will walk through and establish new watches
        # on any new nodes.

        return self._cache[path]

    @_health_check
    def _create_datawatch(self, node):
        """Creates a new DataWatch object for 'node'.

        This method checks if an existing DataWatch has already been created
        for a particular path. If it has, it just exits out because that
        DataWatch should take care of updating the local cache and triggering
        callbacks.

        If the DataWatch does not exist, we create one and save it in our lcoal
        dict so that we know not to do it again. If no existing data for the
        supplied node exists, we assume were being called by _update_node_list
        and that it will handle running the callback function. Otherwise, we
        check if the data has indeed changed, and if so we trigger the
        callback function.

        Args:
            node: String representing the full node-path to watch
        """

        if not node in self._data_watches:
            # Make sure that we only create this callback once for this node
            self._data_watches.append(node)

            # This is the first time we've asked for data about this node,
            # so we will register a watcher on it that handles updating the
            # local cache if the node data changes.
            @self._zk.DataWatch(node)
            def _update_node_data(data, stat):
                # Make sure that self._cache[directory] exists before we try
                # to add some node data to it.
                directory, nodename = split(node)
                if not directory in self._cache:
                    self._cache[directory] = dict()

                # If the node has been deleted, just get out of this function
                if not data and not stat:
                    return 

                # Take in our updated data and decode it
                decoded = funcs.decode(data)

                # See if our data changed, or if its the exact same
                if not nodename in self._cache[directory]:
                    # Assume this is the first time we're being run, and
                    # therefore we will not trigger the callbacks. They should
                    # get triggered by the _update_node_list() function that
                    # initiated us.
                    self._cache[directory][nodename] = decoded
                    trigger_callback = False
                else:
                    if self._cache[directory][nodename] == decoded:
                        self.log.debug('Node data for %s is the same as what '
                                       'we already had registered. Not '
                                       'executing callbacks.' % node)
                        return
                    else:
                        # Update our local cache data
                        self._cache[directory][nodename] = decoded
                        trigger_callback = True

                if trigger_callback:
                    # Execute any callbacks on the path that this node is in
                    self.log.debug('New node data for %s (%s) has triggered '
                                   'the callbacks.' % (node, decoded))
                    self._execute_callbacks(directory)

    @_health_check
    def _get_node_from_provider(self, node):
        """Returns the data from the node registered at path"""

        # Check whether we've got an existing DataWatch on this path
        self._create_datawatch(node)

        # The DataWatch callback only runs after data has been changed, not
        # on the initial registration. We need to manually fetch the data
        # once here.
        directory, nodename = split(node)
        return self._cache[directory][nodename]

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
