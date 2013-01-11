===================
nd_service_registry
===================

*`nd_service_registry`* is a Python module that provides a simple way to leverage
*`Apache Zookeeper`* as a dynamic configuration and service registry.

The goal of the package is to provide a single foundational class that can be
leveraged by any Python program for registering and monitoring services through
Zookeeper.

The current use cases are:
* Register a server providing a service
* Retrieve a list of servers providing a particular service
* Execute callback methods whenever a service list changes

The main benefit of using this module is if you have several different tools
in your network that all need to interact with Zookeeper in a common way. The
most common functions are handled in this singular module allowing you to focus
on your own app development more.

Installation
------------

To install, run ::

    python setup.py install

or ::

    pip install nd_service_registry

Instantiating a KazooServiceRegistry module
-------------------------------------------

Create a logger object::

    >>> import logging
    >>> logger = logging.getLogger()
    >>> logger.setLevel(logging.DEBUG)
    >>> handler = logging.StreamHandler()
    >>> logger.addHandler(handler)

To create your initial connection object::

    >>> from nd_service_registry import KazooServiceRegistry
    >>> nd = KazooServiceRegistry()

The KazooServiceRegistry object is a child of nd_service_registry that conforms 
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

Django use
----------

We wrote this code to be easy to use in Django. Here's a (very brief) version
of the module we use in Django to simplify use of 'nd_service_registry'.

<your django tree>/foo/service_registry_utils.py::

    import nd_service_registry
    from django.conf import settings
    
    _service_registry = None
    
    def get_service_registry():
    global _service_registry
        if not _service_registry:
            server = "%s:%s" % (settings.SERVICEREGISTRY_PARAMS['SERVER'],
                                settings.SERVICEREGISTRY_PARAMS['PORT'])
            _service_registry = nd_service_registry.KazooServiceRegistry(
                server=server,
                lazy=True,
                readonly=True,
                timeout=settings.SERVICEREGISTRY_PARAMS['TIMEOUT'],
                cachefile=settings.SERVICEREGISTRY_PARAMS['CACHEFILE'])
        return _service_registry
    
    def get(path, callback=None, wait=None):
        if not wait:
            return get_service_registry().get(path, callback=callback)
        begin = time.time()
        while time.time() - begin <= wait:
            data = get_service_registry().get(path)
            if len(data['children']) > 0:
                if callback:
                    get_service_registry().add_callback(path, callback=callback)
                return data
            time.sleep(0.1)
        return get_service_registry().get(path, callback=callback)

Example use in your code::

    >>> from nextdoor import service_registry_utils
    >>> def do_something(data):
    ...     print "New server data: %s" % data
    ... 
    >>> service_registry_utils.get('/services/staging/uswest2/memcache',
    ...                            callback=do_something)
    New server data: { 'path': '/services/staging/uswest2/memcache',
                       'stat': ZnodeStat(czxid=8589934751, mzxid=8589934751,
                                         ctime=1354785240728, mtime=1354785240728,
                                         version=0, cversion=45, aversion=0,
                                         ephemeralOwner=0, dataLength=0, numChildren=1,
                                         pzxid=30064903926),
                       'data': None,
                       'children': { u'ec2-123-123-123-123.us-west-2.compute.amazonaws.com:11211':
                                       {u'created': u'2013-01-08 16:51:12', u'pid': 3246, }
                                   }
                       }

Warning: LC_ALL and LANG settings
  Due to an unknown bug, if Django cannot find your LC_ALL LOCALE settings
  (which often default to 'C'), 'nd_service_registry' or 'kazoo' crash and
  burn during the init phase. Its uknown why at this point, but we've found
  that its best to 'unset LC_ALL' and set 'LANG=en_US:UTF-8' (or some other
  valid setting) before you start up your Django app.

  If you use Celery, set these options in */etc/default/celeryd*.

  If you use uWSGI, set them in your uWSGI config file.


Connection Handling
-------------------

The ServiceRegistry object tries everything that it can to make sure that
the backend Zookeeper connection is always up and running.

Fork Behavior
  In the event that your code has created an ServiceRegistry object but then
  gone and forked the process (celery, as an example), we do our best to
  detect this and re-create the connection, watchers and registrations.

  When we detect a fork (more on that below), we re-create our Zookeeper
  connection, and then re-create all Watcher and Registration objects as well.

Fork Detection
  Detecting the fork is extremely tricky... we can only really detect it when
  you call the module for new data. This means that if you have created a
  Watcher or Registration object, those objects will not be aware of the fork
  (and thus the loss of their connection to Zookeeper) until you make another
  call to them.

  Because of this, I strongly recommend that if you can detect the fork from
  within your application (Django signals perhaps?), you should immediately call
  the *rebuild()* method on your ServiceRegistry object.::

      >>> from nd_service_registry import KazooServiceRegistry
      >>> k = KazooServiceRegistry()
      >>> do_fork()
      >>> k.rebuild()

Exceptions
----------

The goal of this module is to be as self-contained as possible and require
as little code in your app as possible. To that end, we *almost never* raise
an Exception once the module is loaded up and connected.

We do raise a few exceptions, and each one is documented here. Whenever we
can though, we instead just *return False* as a way of indicating that we were
unable to perform your command now ... but that we will take care of it later.
Whenever we do this, we throw a WARNING log message as well.

nd_service_registry.exceptions.NoConnection
    Thrown if you attempt any operation that requires immediate access to the
    backend Zookeeper service. Either a *set()* operation, or a *get()*
    operation on a path for the first time.

    Also thrown during initial connection to Zookeeper, if *lazy=False*.

    (It should be noted, a *get()* will actually return the cached results even
    if Zookeeper is down. This allows the service to fail temporarily in the
    background but your app is still able to get the 'last known' results.)

nd_service_registry.exceptions.ReadOnly
    If *readonly=True*, any operation that would result in a 'write' will throw
    this exception. Most notably, a *set()* operation will fail with this
    exception if *readonly=True*.

API Documentation
-----------------

Detailed implementation details and instructions are in the individual
library files.
