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

"""Provides functions for retrieving server lists from our registry.

Configuration:

This module has a default configuration and can be used right away, but you can
also override the SERVER, PORT, TIMEOUT and CACHEFILE settings just by
importing the module and setting them. ie in the settings.py file:

    from nd_service_registry.contrib.django import utils as ndsr
    ndsr_utils.PORT = 2182

Typical usage:

    >>> from nd_service_registry.contrib.django import utils as ndsr
    >>> def do_something(data):
    >>>     print "my nodes changed!"
    >>>     print data['children']
    >>> ndsr.get('/services/staging/uswest1/rabbitmq', callback=do_something)

The callback registration above will always pass a dict of nodes to
whatever method was registered.
"""
from __future__ import absolute_import

import logging
import time

import nd_service_registry

__author__ = 'matt@nextdoor.com (Matt Wise)'

# Service Registry default settings. These are set as package level constants
# (as opposed to being stored as Django settings) intentionally so that the
# actual Django settings module can use this code to get data during the
# startup of your Django app.
SERVER = '127.0.0.1'
PORT = '2181'
TIMEOUT = 15
CACHEFILE = '/tmp/serviceregistry.cache'

_service_registry = None
log = logging.getLogger(__name__)


def get_service_registry():
    """Gets or creates a nd_service_registry object and returns it.

    If the object does not exist, it creates it. If it exists, it returns
    the object as-is. The object is created as a global singleton so that
    a single process of your Django app never opens more than one connection
    to Zookeeper.

    Returns:
        A KazooServiceRegistry object.
    """

    global _service_registry
    if not _service_registry:
        server = "%s:%s" % (SERVER, PORT)

        log.info('Initializing ServiceRegistry object...')
        _service_registry = nd_service_registry.KazooServiceRegistry(
            server=server,
            lazy=True,
            readonly=True,
            timeout=TIMEOUT,
            cachefile=CACHEFILE)
    return _service_registry


def get(path, callback=None, wait=None):
    """Returns a list of nodes/data for a given path.

    Returns a list of node data (and child nodes) for the supplied path
    via the nd_service_registry.get() method. If a callback is supplied, it
    also registers the callback with the ServiceRegistry.

    The ServiceRegistry will execute callbacks immediately, so if you supply
    one, you do not need to also capture the returned data. Example usage:

        >>> def printme(data):
        >>>     print data['children']
        >>> sr.get('/foo', callback=printme)

    Important Note:
        nd_service_registry may execute your callbacks twice -- once for the
        initial 'node watch' (on /foo), and a second time on the children
        watch (on /foo/*). Your code MUST be able to accept an empty
        children{} dict, and then re-configure properly when the children{}
        dict is updated.

    Args:
        path: (string) Path to the Zookeeper data you wish to retrieve
        callback: optional function to execute when the data changes
        wait: (float/int) Time-in-seconds to wait for real 'child' data to
              return. This provides a way to 'block' your object from
              finishing its init until either the timeout has hit, or there
              are at least 1 children being reported from nd_service_registry.

    Returns:
        A dict() from nd_service_registry.get() that looks roughly like this:

            {'children': {u'staging-mc1-uswest2-i-2dfa181e:11211':
                             {u'created': u'2012-12-22 19:49:54',
                              u'pid': 11167,
                               u'zone': u'us-west-2b'}},
             'data': None,
             'path': '/services/staging/uswest2/memcache',
             'stat': ZnodeStat(czxid=8589934751L, mzxid=8589934751L,
                               ctime=1354785240728L, mtime=1354785240728L,
                               version=0, cversion=17, aversion=0,
                               ephemeralOwner=0, dataLength=0, numChildren=1,
                               pzxid=17180055902L)}
    """

    if not wait:
        return get_service_registry().get(path, callback=callback)

    # In the event that returning 'no' nodes initially might be a bad thing,
    # 'wait' provides a way to block on this get() call until at least 1
    # server has been added to the list, OR your timeout expires.
    begin = time.time()
    while time.time() - begin <= wait:
        data = get_service_registry().get(path)
        if len(data['children']) > 0:
            if callback:
                get_service_registry().add_callback(path, callback=callback)
            return data
        time.sleep(0.1)

    # Regardless of the last state of 'data' above, always return the most
    # recent possible data from the nd_service_registry object itself.
    log.info('Waited %f(s) to retrieve server list for %s. Returning whatever'
             'we have now.' % (time.time() - begin, path))
    return get_service_registry().get(path, callback=callback)
