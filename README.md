# Nextdoor Service Registry

*nd_service_registry* is a Python module that provides a simple way to leverage
*Apache Zookeeper* as a dynamic configuration and service registry.

The goal of the package is to provide a single foundational class that can be
leveraged by any Python program for registering and monitoring services through
Zookeeper.

The current use cases are:

 * Register a server providing a service (and subsequently retrieve them)
 * Store a simple dict/key value in Zookeeper (and retreive it)
 * Execute callback methods whenever any node list or data setting changes
 * Get a temporary (but global!) lock while you do something

The main benefit of using this module is if you have several different tools
in your network that all need to interact with Zookeeper in a common way. The
most common functions are handled in this singular module allowing you to focus
on your own app development more.

## Installation

To install, run

    python setup.py install

or

    pip install nd_service_registry

## Usage

### Instantiating KazooServiceRegistry

Create a logger object

    >>> import logging
    >>> logging.basicConfig(level=logging.DEBUG)

To create your initial connection object

    >>> import nd_service_registry
    >>> nd = nd_service_registry.KazooServiceRegistry()

The KazooServiceRegistry object is a child of nd\_service\_registry that
conforms to our ServiceRegistry specs, while leveraging Kazoo as the backend.
The object handles all of your connection states - there is no need to
start/stop or monitor the connection state at all.

### Registering Data

#### Ephemeral Nodes

Registering a node in Zookeeper thats Ephemeral (disappears when the node
goes offline, or if the connection is lost) is highly useful for keeping
track of which servers are offering specific services.

    >>> nd.set_node('/production/ssh/server1:22')
    >>> nd.set_node('/production/ssh/server2:22')
    >>> nd.set_node('/production/ssh/server3:22')
    >>> nd.set_node('/production/web/server2:22',
                    data={'type': 'apache'})
    >>> nd.get('/production/ssh')
    {'children': [u'server1:22', u'server2:22', u'server3:22' ],
     'data': None,
     'path': '/production/ssh',
     'stat': ZnodeStat(czxid=27, mzxid=27, ctime=1355533229452,
                       mtime=1355533229452, version=0, cversion=5,
                       aversion=0, ephemeralOwner=0, dataLength=0,
                       numChildren=3, pzxid=45)}


To register the host as providing a particular service

    >>> nd.set_node('/services/ssh/server1:22', data={ 'foo': 'bar'})

Getting a list of servers at a path

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

#### Arbitrary Data

When you want to store arbitrary (but simple!) data objects in, its simple!

    >>> sr.set_data('/config/my_app', data={'state': 'enabled'})
    True
    >>> sr.get('/config/my_app')
    {'children': []
     'data': {u'created': u'2014-06-03 13:37:53',
              u'pid': 2546,
              u'state': u'enabled'},
     'path': '/config/my_app',
     'stat': ZnodeStat(czxid=76, mzxid=77, ctime=1401827873764,
                       mtime=1401827873766, version=1, cversion=0,
                       aversion=0, ephemeralOwner=0, dataLength=62,
                       numChildren=0, pzxid=76)}

### Distributed Locks

One of Zookeepers great features is using it as a global lock manager. We provide
two models for getting a lock. In one model, your lock is only active as long as
your code is running

    >>> with nd.get_lock('/foo', simultaneous=1):
    ...      <do some work>
    ...
    >>>

Another example is explicitly locking a path for some period of time, then
releasing it explicitly (eg, locking during one method, and waiting for an
entirely different method to handle the unlock)

    >>> nd.acquire_lock('/foo', simultaneous=1)
    >>> <do your work... >
    >>> nd.release_lock('/foo')

### Django

We initially wrote this code to simplify our use of Zookeeper in Django.
Included is a very simple Django utility package that makes it dead simple
to use Zookeeper inside Django for retreiving information. To use, add
the following to your *settings* module in Django:

    from nd_service_registry.contrib.django import utils as ndsr
    ndsr.SERVER = '127.0.0.1'
    ndsr.PORT = '2181'
    ndsr.TIMEOUT = 5
    ndsr.CACHEFILE = '/tmp/serviceregistry.cache'

Example usage to grab a single string and use it as a setting:
    
    MY_CRED = ndsr.get('/creds/some_cred')['data']['mycred']

Example use in your code, calling a method every time a value is updated
in Zookeeper: 

    >>> def do_something(data):
    ...     print "New server data: %s" % data
    ...
    >>> ndsr.get('/services/staging/uswest2/memcache', callback=do_something)
    New server data: { 'path': '/services/staging/uswest2/memcache',
                       'stat': ZnodeStat(czxid=8589934751, mzxid=8589934751,
                                         ctime=1354785240728,
                                         mtime=1354785240728, version=0,
                                         cversion=45, aversion=0,
                                         ephemeralOwner=0, dataLength=0,
                                         numChildren=1, pzxid=30064903926),
                       'data': None,
                       'children': { u'ec2-123-123-123-123.us-west-2:11211': {
                                       u'created': u'2013-01-08 16:51:12',
                                       u'pid': 3246
                                     }
                                   }
                       }

Warning: LC\_ALL and LANG settings
  Due to an unknown bug, if Django cannot find your LC\_ALL LOCALE settings
  (which often default to 'C'), *nd_service_registry* or *kazoo* crash and
  burn during the init phase. Its unknown why at this point, but we've found
  that its best to *unset LC\_ALL* and set *LANG=en_US:UTF-8* (or some other
  valid setting) before you start up your Django app.

  If you use Celery, set these options in */etc/default/celeryd*.

  If you use uWSGI, set them in your uWSGI config file.

  Running the Django shell

      # unset LC_ALL; LANG=en_US:UTF-8 python manage.py shell


## Connection Handling

The ServiceRegistry object tries everything that it can to make sure that
the backend Zookeeper connection is always up and running.

### Fork Behavior

In the event that your code has created an ServiceRegistry object but then
gone and forked the process (celery, as an example), we do our best to
detect this and re-create the connection, watchers and registrations.

When we detect a fork (more on that below), we re-create our Zookeeper
connection, and then re-create all Watcher and Registration objects as well.

### Fork Detection

Detecting the fork is extremely tricky... we can only really detect it when
you call the module for new data. This means that if you have created a
Watcher or Registration object, those objects will not be aware of the fork
(and thus the loss of their connection to Zookeeper) until you make another
call to them.

Because of this, I strongly recommend that if you can detect the fork from
within your application (Django signals perhaps?), you should immediately call
the *rebuild()* method on your ServiceRegistry object.

    >>> from nd_service_registry import KazooServiceRegistry
    >>> k = KazooServiceRegistry()
    >>> do_fork()
    >>> k.rebuild()

## Exceptions

The goal of this module is to be as self-contained as possible and require
as little code in your app as possible. To that end, we *almost never* raise
an Exception once the module is loaded up and connected.

We do raise a few exceptions, and each one is documented here. Whenever we
can though, we instead just *return False* as a way of indicating that we were
unable to perform your command now ... but that we will take care of it later.
Whenever we do this, we throw a WARNING log message as well.

### nd\_service\_registry.exceptions.NoConnection

Thrown if you attempt any operation that requires immediate access to the
backend Zookeeper service. Either a *set()* operation, or a *get()*
operation on a path for the first time.

Also thrown during initial connection to Zookeeper, if *lazy=False*.

(It should be noted, a *get()* will actually return the cached results even
if Zookeeper is down. This allows the service to fail temporarily in the
background but your app is still able to get the *last known* results.)

### nd\_service\_registry.exceptions.ReadOnly

If *readonly=True*, any operation that would result in a *write* will throw
this exception. Most notably, a *set()* operation will fail with this
exception if *readonly=True*.

## API Documentation

Detailed implementation details and instructions are in the individual
library files.

## Development

### Unit Tests

Running them

    $ python setup.py test
    running test
    test_creates_simple_json_outputformat (nd_service_registry.bin.ndsr.get_tests.GetTests) ... ok
    ...
    ...
    test_default_data_produces_expected_dict (nd_service_registry.funcs_tests.FuncsTests) ... ok
    test_encode_creates_dict_from_single_string (nd_service_registry.funcs_tests.FuncsTests) ... ok
    Ran 15 tests in 0.108s

    OK

### Integration Tests

Ingegration tests download and install Zookeeper in a temporary path in your
workspace.  They can be executed like this

    $ make integration
    running integration
    ...
    Make sure that the enter/exit functionality works in non-blocking ... ok
    Test that a blocking Lock works ... ok
    test_decode_converts_json_to_dict (nd_service_registry.funcs_tests.FuncsTests) ... ok

    ----------------------------------------------------------------------
    Ran 19 tests in 3.360s

    OK

## Apps using nd\_service\_registry

 * [zk_monitor](http://github.com/nextdoor/zkmonitor): Zookeeper Path Monitoring Agent
 * [zk_watcher](http://github.com/nextdoor/zkwatcher): Registers third-party (memcache/etc)
   services in Zookeeper.
