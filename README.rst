=================
ndServiceRegistry
=================

`ndServiceRegistry` is a Python module that provides a simple way to leverage
`Apache Zookeeper` as a dynamic configuration and service registry.

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

Create a logging.Logger object::

    >>> import logging
    >>> logger = logging.getLogger()
    >>> logger.setLevel(logging.DEBUG)
    >>> handler = logging.StreamHandler()
    >>> logger.addHandler(handler)

To create your initial connection object::

    >>> from ndServiceRegistry import KazooServiceRegistry
    >>> nd = KazooServiceRegistry()

The KazooServiceRegistry object is a sub-object that conforms to our
ServiceRegistry specs, whlie leveraging Kazoo as the backend. The object
handles all of your connection states - there is no need to start/stop
or monitor the connection state at all.

Using KazooServiceRegistry to register a service
------------------------------------------------

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

API Documentation
-----------------

Detailed implementation details and instructions are in the individual
library files.
