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

"""Commonly used functions for nd_service_registry

Copyright 2012 Nextdoor Inc."""

__author__ = 'matt@nextdoor.com (Matt Wise)'

import cPickle as pickle
import json
import logging
import os
import tempfile
import time

log = logging.getLogger(__name__)


def encode(data=None):
    """Converts a data dict into a storable string.

    Takes a supplied dict object and converts it into a storable JSON
    string for storage in the backend. Mimics the zc.zk.ZooKeeper encode
    method, so its compatible with zc.zk clients.

    Args:
        data: A dict of key:value pairs to store with the node.

    Returns:
        A JSON string with the supplied data as well as some default data"""

    # Check if the data is a single string. If so, turn it into a dict
    if isinstance(data, basestring):
        new_data = {}
        new_data['string_value'] = data
        data = new_data

    # Add in the default data points that we generate internally
    if data:
        data = dict(data.items() + default_data().items())
    else:
        data = default_data()

    # If default_data() returns nothing, and the user supplied either
    # a single string, or a dict where the only key is string_value,
    # then just pass the araw string_value key to back. No encoding necessary.
    if len(data) == 1 and 'string_value' in data:
        return data['string_value']

    if data:
        return json.dumps(data, separators=(',', ':'))
    else:
        return ''


def decode(data):
    """Converts string data from JSON format back into a dict.

    This decodes the data retrieved from a node (from the _get_node())
    method and converts it into a dict. If its a pure string, we create
    a dict. If its in JSON format (likely created by _encode(), we
    convert it into a dict and return it.

    Args:
        data: A string of either pure text or JSON-text.

    Returns:
        A dict that represents the supplied data"""

    if not data:
        return None

    # Strip incoming data of new lines
    s = data.strip()

    if not s:
        data = {}
    elif s.startswith('{') and s.endswith('}'):
        try:
            data = json.loads(s)
        except Exception, e:
            data = dict(string_value=data)
    else:
        data = dict(string_value=data)
    return data


def default_data():
    """Default data that all party-members share."""
    data = {}
    data['pid'] = os.getpid()
    data['created'] = time.strftime('%Y-%m-%d %H:%M:%S')

    return data


def save_dict(data, path):
    """Saves a copy of our cache to a pickle file.

    This code should never fail to write to disk. If it fails, we allow
    the error to occur properly and the app should die off.

    Args:
        data: Dictionary of data to save
        path: Filename to save 'data' to

    Returns:
        true: supplied data was saved properly to supplied file
        False: was unable to save dict  properly
    """

    if not path:
        raise Exception('No \'path\' path was supplied to save dict to.')

    # Get a logger
    log.info('Saving supplied data to pickle file [%s]' % path)

    # Get a copy of our existing saved-dictionary first. Append our current
    # cache to it.
    cache = {}
    try:
        cache = pickle.load(open(path, 'rb'))
    except (IOError, EOFError), e:
        log.warning('Could not load existing cache (%s): %s' %
                   (path, e))

    # Join our existing disk cache with the in-memory cache and save the whole
    # bundle.
    cache.update(data)

    # Create a random file and save our cache to it. This should avoid the race
    # condition of multiple processes saving to the same file at the same time.
    try:
        fileno, filename = tempfile.mkstemp()
        fd = os.fdopen(fileno, 'wb')
        pickle.dump(cache, fd)
        fd.close()
        os.rename(filename, path)
    except Exception, e:
        log.warning('Could not save cache (%s): %s' % (path, e))
        return False

    return True


def load_dict(file):
    """Load up our cached dict from a pickle file.

    This function raises an error if the dict cannot be loaded up.
    Otherwise we set the cache up with our dict import.

    Returns:
        dict: The dict that has been loaded from the supplied filename
    """

    if not file:
        raise Exception('No \'file\' path was supplied to save dict to.')

    log.info('Loading up dictionary from file [%s]' % file)
    cache = {}
    try:
        cache = pickle.load(open(file, 'rb'))
    except (IOError, EOFError), e:
        log.info('Could not load %s pickle file:' % file)
        log.info(e)
        raise e

    log.info('Dict object file loaded properly [%s]' % file)
    return cache
