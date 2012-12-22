=================
ndServiceRegistry
=================

*`ndServiceRegistry`* is a Python module that provides a simple way to leverage
*`Apache Zookeeper`* as a dynamic configuration and service registry.

The goal of the package is to provide a single foundational class that can be
leveraged by any Python program for registering and monitoring services through
Zookeeper.

The current use cases are:
- Register a server providing a service
- Retrieve a list of servers providing a particular service
- Execute callback methods whenever a service list changes

The main benefit of using this module is if you have several different tools
in your network that all need to interact with Zookeeper in a common way. The
most common functions are handled in this singular module allowing you to focus
on your own app development more.

Installation
------------

To install, run ::

    python setup.py install

or ::

    pip install ndServiceRegistry

Instantiating a KazooServiceRegistry module
-------------------------------------------

Create a logger object::

    >>> import logging
    >>> logger = logging.getLogger()
    >>> logger.setLevel(logging.DEBUG)
    >>> handler = logging.StreamHandler()
    >>> logger.addHandler(handler)

To create your initial connection object::

    >>> from ndServiceRegistry import KazooServiceRegistry
    >>> nd = KazooServiceRegistry()

The KazooServiceRegistry object is a child of ndServiceRegistry that conforms 
to our ServiceRegistry specs, whlie leveraging Kazoo as the backend. The
object handles all of your connection states - there is no need to start/stop
or monitor the connection state at all.

Basic use
---------

To register the host as providing a particular service::

    >>> nd.set_node('/services/ssh/server1:22', data={ 'foo': 'bar'})

Getting a list of servers at a path::

    >>> nd.get('/services/ssh')
    {'children': {u'server1:22': {u'foo': u'bar',
                                  u'created': u'2012-12-15 00:45:03',
                                  u'pid': 10733}},
     'data': None,
     'path': '/services/ssh',
     'stat': ZnodeStat(czxid=6, mzxid=6, ctime=1355532303688,
                       mtime=1355532303688, version=0, cversion=1,
                       aversion=0, ephemeralOwner=0, dataLength=0,
                       numChildren=1, pzxid=7)}

Failure Handling
----------------

The goal of this module is to be as self-contained as possible and require
as little code in your app as possible. To that end, we *almost never* raise
an Exception once the module is loaded up and connected.

We do raise a few exceptions, and each one is documented here. Whenever we
can though, we instead just *return False* as a way of indicating that we were
unable to perform your command now ... but that we will take care of it later.
Whenever we do this, we throw a WARNING log message as well.

ndServiceRegistry.exceptions.NoConnection
    Thrown if you attempt any operation that requires immediate access to the
    backend Zookeeper service. Either a *set()* operation, or a *get()*
    operation on a path for the first time.

    Also thrown during initial connection to Zookeeper, if *lazy=False*.

    (It should be noted, a *get()* will actually return the cached results even
    if Zookeeper is down. This allows the service to fail temporarily in the
    background but your app is still able to get the 'last known' results.)

ndServiceRegistry.exceptions.ReadOnly
    If *readonly=True*, any operation that would result in a 'write' will throw
    this exception. Most notably, a *set()* operation will fail with this
    exception if *readonly=True*.

API Documentation
-----------------

Detailed implementation details and instructions are in the individual
library files.
