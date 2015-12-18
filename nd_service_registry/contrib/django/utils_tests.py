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
from __future__ import absolute_import

import unittest
from mock import patch
import mock

from nd_service_registry.contrib.django import utils

__author__ = 'matt@nextdoor.com.com (Matt Wise)'


class DjangoUtilsTests(unittest.TestCase):

    # A flag for filtering nose tests
    unit = True

    def setUp(self):
        utils._service_registry = None

    @patch('nd_service_registry.KazooServiceRegistry')
    def test_get_service_registry(self, mock_sr):
        # Mock up some special connection settings just to validate
        # that they get passed through to the ServiceRegistry object
        # creation properly.
        utils.PORT = 1234
        utils.SERVER = '127.0.0.2'
        utils.TIMEOUT = 10
        utils.CACHEFILE = '/tmp/unittest'

        # Request a ServiceRegistry object once and save it
        sr1 = utils.get_service_registry()

        # Now, request it again and make sure we get back
        # a reference to the first one
        sr2 = utils.get_service_registry()
        self.assertEquals(sr1, sr2)

        # Ensure that the mocked method was called with the right# options
        mock_sr.assert_called_once_with(cachefile='/tmp/unittest',
                                        lazy=True,
                                        readonly=True,
                                        timeout=10,
                                        server='127.0.0.2:1234')

    def test_get(self):
        mocked_results = {'children': {u'staging-mc1-uswest2-i-2dfa181e:123': {
            u'created': u'2012-12-22 19:49:54', u'pid': 11167,
                        u'zone': u'us-west-2b'}},
            'data': None,
            'path': '/services/staging/uswest2/memcache',
            'stat': None}

        # Mock up a ServiceRegistry object so we can track the calls
        mocked_sr = mock.MagicMock()
        mocked_sr.get.return_value = mocked_results
        utils._service_registry = mocked_sr

        # Simulate a a basic get with no callback, and no wait.
        utils.get('/foo')
        mocked_sr.get.assert_called_with('/foo', callback=None)

    def test_get_with_callback(self):
        mocked_results = {'children': {u'staging-mc1-uswest2-i-2dfa181e:123': {
            u'created': u'2012-12-22 19:49:54', u'pid': 11167,
                        u'zone': u'us-west-2b'}},
            'data': None,
            'path': '/services/staging/uswest2/memcache',
            'stat': None}

        # Mock up a ServiceRegistry object so we can track the calls
        mocked_sr = mock.MagicMock()
        mocked_sr.get.return_value = mocked_results
        utils._service_registry = mocked_sr

        # Create a simple callback method
        def callme(data):
            self.assertEquals(data['path'], mocked_results['path'])

        # Simulate a a basic get with no callback, and no wait.
        utils.get('/foo', callback=callme)
        mocked_sr.get.assert_called_with('/foo', callback=callme)

    def test_get_with_callback_and_wait(self):
        mocked_results_empty = {'children': {},
                                'data': None,
                                'path': '/services/staging/uswest2/memcache',
                                'stat': None}

        mocked_results = {'children': {u'staging-mc1-uswest2-i-2dfa181e:123': {
            u'created': u'2012-12-22 19:49:54', u'pid': 11167,
                        u'zone': u'us-west-2b'}},
            'data': None,
            'path': '/services/staging/uswest2/memcache',
            'stat': None}

        # Mock up a ServiceRegistry object and use the side_effect setting
        # of MagicMock to return empty results twice before returning valid
        # results with a real child entry on the third try. This allows us to
        # fake the 'waiting' for Zookeeper to respond.
        mocked_sr = mock.MagicMock()
        mocked_sr.add_callback.return_value = True
        mocked_sr.get.side_effect = [mocked_results_empty,
                                     mocked_results_empty,
                                     mocked_results]
        utils._service_registry = mocked_sr

        # Create a simple callback method
        def callme(data):
            data

        # Simulate a a basic get with no callback, and no wait.
        utils.get('/foo', callback=callme, wait=1)
        mocked_sr.get.assert_called_with('/foo')
        mocked_sr.add_callback.assert_called_with('/foo', callback=callme)

    def test_get_with_wait_that_times_out(self):
        mocked_results_empty = {'children': {},
                                'data': None,
                                'path': '/services/staging/uswest2/memcache',
                                'stat': None}

        # Mock up a ServiceRegistry object and use the side_effect setting
        # of MagicMock to return empty results twice before returning valid
        # results with a real child entry on the third try. This allows us to
        # fake the 'waiting' for Zookeeper to respond.
        mocked_sr = mock.MagicMock()
        mocked_sr.add_callback.return_value = True
        mocked_sr.get.side_effect = [mocked_results_empty,
                                     mocked_results_empty,
                                     mocked_results_empty,
                                     mocked_results_empty]
        utils._service_registry = mocked_sr

        # Create a simple callback method
        def callme(data):
            data

        # Simulate a a basic get with no callback, and pass in a wait value of
        # zero. This means we will only iterate once calling the get() method
        # before failing. This tests the fail case, but doesn't take any real
        # time during our tests.
        utils.get('/foo', callback=callme, wait=0.1)
        mocked_sr.get.assert_has_calls([
            mock.call('/foo'),
            mock.call('/foo', callback=callme)])
