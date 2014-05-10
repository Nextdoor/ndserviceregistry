import mock
import unittest

from nd_service_registry import registration


class RegistrationTests(unittest.TestCase):

    # A flag for filtering nose tests
    unit = True

    def test_data_node(self):
        zk = mock.Mock()
        data = {'hello': 'world'}
        node = registration.DataNode(zk, '/service/path', data)
        self.assertEqual(node._data, data)
        self.assertFalse(node._ephemeral)
        self.assertEqual(len(node._watcher._callbacks), 0)

    def test_ephemeral_node(self):
        zk = mock.Mock()
        data = {'hello': 'world'}
        node = registration.EphemeralNode(zk, '/service/path', data)
        self.assertEqual(node._data, data)
        self.assertTrue(node._ephemeral)
        self.assertEqual(len(node._watcher._callbacks), 1)
