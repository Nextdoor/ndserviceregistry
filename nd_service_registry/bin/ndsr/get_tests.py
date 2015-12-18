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

import json
import unittest
from mock import patch

import yaml

from nd_service_registry.bin.ndsr.get import Get

__author__ = 'me@ryangeyer.com (Ryan J. Geyer)'


class GetTests(unittest.TestCase):
    # A flag for filtering nose tests
    unit = True

    @patch('nd_service_registry.KazooServiceRegistry')
    def test_creates_simple_yaml_outputformat(self, mock_kazoo_class):
        retval = {'path': '/foo'}
        mock_kazoo_class.get.return_value = retval
        fauxGflags = type('foo', (object,),
                          {"quiet": True,
                           "server": None,
                           "username": None,
                           "password": None,
                           "outputformat": "yaml",
                           "data": False,
                           "recursive": False})
        expected = {"/foo": {}}
        get = Get(mock_kazoo_class)
        output = get.execute([], fauxGflags)
        assert output == yaml.dump(expected, default_flow_style=False)

    @patch('nd_service_registry.KazooServiceRegistry')
    def test_creates_simple_json_outputformat(self, mock_kazoo_class):
        retval = {'path': '/foo'}
        mock_kazoo_class.get.return_value = retval
        fauxGflags = type('foo', (object,),
                          {"quiet": True,
                           "server": None,
                           "username": None,
                           "password": None,
                           "outputformat": "json",
                           "data": False,
                           "recursive": False})
        expected = {"/foo": {}}
        get = Get(mock_kazoo_class)
        output = get.execute([], fauxGflags)
        assert output == json.dumps(expected, sort_keys=True, indent=4,
                                    separators=(',', ': '))

    @patch('nd_service_registry.KazooServiceRegistry')
    def test_creates_simple_dir_outputformat(self, mock_kazoo_class):
        retval = {'path': '/foo'}
        mock_kazoo_class.get.return_value = retval
        fauxGflags = type('foo', (object,),
                          {"quiet": True,
                           "server": None,
                           "username": None,
                           "password": None,
                           "outputformat": "dir",
                           "data": False,
                           "recursive": False})
        get = Get(mock_kazoo_class)
        output = get.execute([], fauxGflags)
        assert output == "/foo"

    @patch('nd_service_registry.KazooServiceRegistry')
    def test_yaml_includes_data_on_data_flag(self, mock_kazoo_class):
        retval = {'path': '/foo', 'data': {'foo': 'bar'}}
        mock_kazoo_class.get.return_value = retval
        fauxGflags = type('foo', (object,),
                          {"quiet": True,
                           "server": None,
                           "username": None,
                           "password": None,
                           "outputformat": "yaml",
                           "data": True,
                           "recursive": False})
        expected = {"/foo": {"data": {"foo": "bar"}}}
        get = Get(mock_kazoo_class)
        output = get.execute([], fauxGflags)
        assert output == yaml.dump(expected, default_flow_style=False)

    @patch('nd_service_registry.KazooServiceRegistry')
    def test_json_includes_data_on_data_flag(self, mock_kazoo_class):
        retval = {'path': '/foo', 'data': {'foo': 'bar'}}
        mock_kazoo_class.get.return_value = retval
        fauxGflags = type('foo', (object,),
                          {"quiet": True,
                           "server": None,
                           "username": None,
                           "password": None,
                           "outputformat": "json",
                           "data": True,
                           "recursive": False})
        expected = {"/foo": {"data": {"foo": "bar"}}}
        get = Get(mock_kazoo_class)
        output = get.execute([], fauxGflags)
        assert output == json.dumps(expected, sort_keys=True, indent=4,
                                    separators=(',', ': '))

    @patch('nd_service_registry.KazooServiceRegistry')
    def test_dir_includes_data_on_data_flag(self, mock_kazoo_class):
        retval = {'path': '/foo', 'data': {'foo': 'bar'}}
        mock_kazoo_class.get.return_value = retval
        fauxGflags = type('foo', (object,),
                          {"quiet": True,
                           "server": None,
                           "username": None,
                           "password": None,
                           "outputformat": "dir",
                           "data": True,
                           "recursive": False})
        get = Get(mock_kazoo_class)
        output = get.execute([], fauxGflags)
        assert output == "/foo"

    @patch('nd_service_registry.KazooServiceRegistry')
    def test_yaml_includes_children_on_recursive_flag(self, mock_kazoo_class):
        foo = {
            'path': '/foo',
            'data': {'foo': 'bar'},
            'children': {'bar': {'foo': 'bar'}}
        }
        foobar = {'path': '/foo/bar', 'data': {'bar': 'baz'}}

        def side_effect(arg):
            arg_value_dict = {'/': foo, '/foo/bar': foobar}
            return arg_value_dict[arg]

        mock_kazoo_class.get.side_effect = side_effect
        fauxGflags = type('foo', (object,),
                          {"quiet": True,
                           "server": None,
                           "username": None,
                           "password": None,
                           "outputformat": "yaml",
                           "data": False,
                           "recursive": True})
        expected = {"/foo": {"children": [{"/foo/bar": {}}]}}
        get = Get(mock_kazoo_class)
        output = get.execute([], fauxGflags)
        assert output == yaml.safe_dump(expected, default_flow_style=False)

    @patch('nd_service_registry.KazooServiceRegistry')
    def test_json_includes_children_on_recursive_flag(self, mock_kazoo_class):
        foo = {
            'path': '/foo',
            'data': {'foo': 'bar'},
            'children': {'bar': {'foo': 'bar'}}
        }
        foobar = {'path': '/foo/bar', 'data': {'bar': 'baz'}}

        def side_effect(arg):
            arg_value_dict = {'/': foo, '/foo/bar': foobar}
            return arg_value_dict[arg]

        mock_kazoo_class.get.side_effect = side_effect
        fauxGflags = type('foo', (object,),
                          {"quiet": True,
                           "server": None,
                           "username": None,
                           "password": None,
                           "outputformat": "json",
                           "data": False,
                           "recursive": True})
        expected = {"/foo": {"children": [{"/foo/bar": {}}]}}
        get = Get(mock_kazoo_class)
        output = get.execute([], fauxGflags)
        assert output == json.dumps(expected, sort_keys=True, indent=4,
                                    separators=(',', ': '))

    @patch('nd_service_registry.KazooServiceRegistry')
    def test_dir_includes_children_on_recursive_flag(self, mock_kazoo_class):
        foo = {
            'path': '/foo',
            'data': {'foo': 'bar'},
            'children': {'bar': {'foo': 'bar'}}
        }
        foobar = {'path': '/foo/bar', 'data': {'bar': 'baz'}}

        def side_effect(arg):
            arg_value_dict = {'/': foo, '/foo/bar': foobar}
            return arg_value_dict[arg]

        mock_kazoo_class.get.side_effect = side_effect
        fauxGflags = type('foo', (object,),
                          {"quiet": True,
                           "server": None,
                           "username": None,
                           "password": None,
                           "outputformat": "dir",
                           "data": False,
                           "recursive": True})
        get = Get(mock_kazoo_class)
        output = get.execute([], fauxGflags)
        assert output == "/foo\n/foo/bar"

    @patch('nd_service_registry.KazooServiceRegistry')
    def test_yaml_includes_grandchildren_on_recursive_flag(self,
                                                           mock_kazoo_class):
        foo = {
            'path': '/foo',
            'data': {'foo': 'bar'},
            'children': {'bar': {'foo': 'bar'}}
        }
        foobar = {
            'path': '/foo/bar',
            'data': {'bar': 'baz'},
            'children': {'baz': 'foo'}
        }
        foobarbaz = {'path': '/foo/bar/baz'}

        def side_effect(arg):
            arg_value_dict = {
                '/': foo,
                '/foo/bar': foobar,
                '/foo/bar/baz': foobarbaz
            }
            return arg_value_dict[arg]

        mock_kazoo_class.get.side_effect = side_effect
        fauxGflags = type('foo', (object,),
                          {"quiet": True,
                           "server": None,
                           "username": None,
                           "password": None,
                           "outputformat": "yaml",
                           "data": False,
                           "recursive": True})
        expected = {
            "/foo": {
                "children": [
                    {
                        "/foo/bar": {
                            "children": [
                                {"/foo/bar/baz": {}}
                            ]
                        }
                    }
                ]
            }
        }
        get = Get(mock_kazoo_class)
        output = get.execute([], fauxGflags)
        assert output == yaml.safe_dump(expected, default_flow_style=False)

    @patch('nd_service_registry.KazooServiceRegistry')
    def test_json_includes_grandchildren_on_recursive_flag(self,
                                                           mock_kazoo_class):
        foo = {
            'path': '/foo',
            'data': {'foo': 'bar'},
            'children': {'bar': {'foo': 'bar'}}
        }
        foobar = {
            'path': '/foo/bar',
            'data': {'bar': 'baz'},
            'children': {'baz': 'foo'}
        }
        foobarbaz = {'path': '/foo/bar/baz'}

        def side_effect(arg):
            arg_value_dict = {
                '/': foo,
                '/foo/bar': foobar,
                '/foo/bar/baz': foobarbaz
            }
            return arg_value_dict[arg]

        mock_kazoo_class.get.side_effect = side_effect
        fauxGflags = type('foo', (object,),
                          {"quiet": True,
                           "server": None,
                           "username": None,
                           "password": None,
                           "outputformat": "json",
                           "data": False,
                           "recursive": True})
        expected = {
            "/foo": {
                "children": [
                    {
                        "/foo/bar": {
                            "children": [
                                {"/foo/bar/baz": {}}
                            ]
                        }
                    }
                ]
            }
        }
        get = Get(mock_kazoo_class)
        output = get.execute([], fauxGflags)
        assert output == json.dumps(expected, sort_keys=True, indent=4,
                                    separators=(',', ': '))

    @patch('nd_service_registry.KazooServiceRegistry')
    def test_dir_includes_grandchildren_on_recursive_flag(self,
                                                          mock_kazoo_class):
        foo = {
            'path': '/foo',
            'data': {'foo': 'bar'},
            'children': {'bar': {'foo': 'bar'}}
        }
        foobar = {
            'path': '/foo/bar',
            'data': {'bar': 'baz'},
            'children': {'baz': 'foo'}
        }
        foobarbaz = {'path': '/foo/bar/baz'}

        def side_effect(arg):
            arg_value_dict = {
                '/': foo,
                '/foo/bar': foobar,
                '/foo/bar/baz': foobarbaz
            }
            return arg_value_dict[arg]

        mock_kazoo_class.get.side_effect = side_effect
        fauxGflags = type('foo', (object,),
                          {"quiet": True,
                           "server": None,
                           "username": None,
                           "password": None,
                           "outputformat": "dir",
                           "data": False,
                           "recursive": True})
        get = Get(mock_kazoo_class)
        output = get.execute([], fauxGflags)
        assert output == "/foo\n/foo/bar\n/foo/bar/baz"
