from __future__ import absolute_import

import os
import json
import mock
import unittest

import six

from nd_service_registry import funcs


class FuncsTests(unittest.TestCase):

    # A flag for filtering nose tests
    unit = True

    def test_encode_adds_extra_properties(self):
        json_data = funcs.encode({"foo": "bar", "baz": "foo"})
        self.assertIn(b'"pid":', json_data)
        self.assertIn(b'"created":"', json_data)

    def test_encode_creates_dict_from_single_string(self):
        to_encode = "String"
        json_data = funcs.encode(to_encode)
        self.assertIn(b'"string_value":"String"', json_data)

    def test_decode_converts_json_to_dict(self):
        result_dict = funcs.decode(
            '{"pid":1,"string_value":"String","created":"2013-11-18 19:37:04"}'
        )
        six.assertCountEqual(self,
                             [u"pid", u"string_value", u"created"],
                             result_dict.keys())
        six.assertCountEqual(self,
                             [1, u"String", u"2013-11-18 19:37:04"],
                             result_dict.values())

    def test_decode_returns_none_on_empty_input(self):
        self.assertEqual(None, funcs.decode(''))

    def test_decode_returns_dict_when_on_non_json_string_input(self):
        self.assertEquals({"string_value": "foo"}, funcs.decode("foo"))

    def test_decode_returns_string_value_dict_on_malformed_json_string_input(
            self):
        json.loads = mock.Mock()
        exception_message = "This should be a more specific exception which \
                            gets caught by the decode function"
        json.loads.side_effect = Exception(exception_message)
        self.assertEquals({"string_value": '{"foo":"bar}'},
                          funcs.decode('{"foo":"bar}'))

    def test_default_data_produces_expected_dict(self):
        default_data = funcs.default_data()
        self.assertIn(u"pid", list(default_data.keys()))
        self.assertIn(u"created", list(default_data.keys()))
        self.assertEqual(os.getpid(), default_data['pid'])
        self.assertRegexpMatches(
            default_data['created'],
            r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'
        )
