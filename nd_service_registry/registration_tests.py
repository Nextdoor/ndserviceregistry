from __future__ import absolute_import

from mock import patch
import mock
import threading
import unittest

from kazoo import exceptions
from kazoo import security

from nd_service_registry import registration
from nd_service_registry import watcher


class TestExc(Exception):
    """Bogus exception"""


class RegistrationBaseTests(unittest.TestCase):
    # A flag for filtering nose tests
    unit = True

    @mock.patch('kazoo.recipe.watchers.DataWatch')
    @mock.patch('kazoo.recipe.watchers.ChildrenWatch')
    @patch.object(watcher.Watcher, 'add_callback')
    def setUp(self, *args, **kwargs):
        self.zk = mock.MagicMock()
        self.reg = registration.RegistrationBase(self.zk, '/unittest/host:22')

    def test_create_node(self):
        self.reg._create_node()
        self.zk.retry.assert_called_once_with(
            self.zk.create,
            '/unittest/host:22',
            value=self.reg._encoded_data, ephemeral=False, makepath=False)

    def test_create_node_with_new_data(self):
        self.reg._encoded_data = '1234'
        self.reg._create_node()
        self.zk.retry.assert_called_once_with(
            self.zk.create,
            '/unittest/host:22',
            value='1234', ephemeral=False, makepath=False)

    def test_create_node_exists_error(self):
        self.zk.retry.side_effect = exceptions.NodeExistsError()
        self.reg._create_node()
        self.zk.retry.assert_called_once_with(
            self.zk.create,
            '/unittest/host:22',
            value=self.reg._encoded_data, ephemeral=False, makepath=False)

    def test_create_node_noauth(self):
        self.zk.retry.side_effect = exceptions.NoAuthError('Boom!')
        self.reg._create_node()
        self.zk.retry.assert_called_once_with(
            self.zk.create,
            '/unittest/host:22',
            value=self.reg._encoded_data, ephemeral=False, makepath=False)

    def test_create_misc_exc(self):
        self.zk.retry.side_effect = TestExc('Oh snap!')
        self.reg._create_node()
        self.zk.retry.assert_called_once_with(
            self.zk.create,
            '/unittest/host:22',
            value=self.reg._encoded_data, ephemeral=False, makepath=False)

    @patch.object(registration.RegistrationBase, '_create_node')
    @patch.object(registration.RegistrationBase, '_create_node_path')
    def test_update_no_node_error(self,
                                  mock_create_node_path,
                                  mock_create_node):
        # The first time _create_node() is called, we raise the exception.
        # After that we just return True so that we can pretent like the
        # subsequent calls in _create() work fine.
        mock_create_node.side_effect = [
            exceptions.NoNodeError('Snap'), True]

        self.reg._update_state(True)

        # The create_node_path method should be executed once.
        mock_create_node_path.assert_called_once_with()

        # The create_node method should be executed twice.
        self.assertEqual(mock_create_node.call_count, 2)

    def test_create_node_path(self):
        self.reg._create_node_path()
        self.zk.retry.assert_called_once_with(self.zk.ensure_path, '/unittest')

        self.reg._path = '/foo/bar/host:22'
        self.reg._create_node_path()
        calls = [
            mock.call(self.zk.ensure_path, '/foo',
                      acl=security.OPEN_ACL_UNSAFE),
            mock.call(self.zk.ensure_path, '/foo/bar')]
        self.zk.retry.assert_has_calls(calls)

        self.reg._path = '/foo/bar/baz/host:22'
        self.reg._create_node_path()
        calls = [
            mock.call(self.zk.ensure_path, '/foo/bar',
                      acl=security.OPEN_ACL_UNSAFE),
            mock.call(self.zk.ensure_path, '/foo/bar/baz')]
        self.zk.retry.assert_has_calls(calls)

        self.reg._path = '/foo/bar/baz/abc/test/host:22'
        self.reg._create_node_path()
        calls = [
            mock.call(self.zk.ensure_path, '/foo/bar/baz/abc',
                      acl=security.OPEN_ACL_UNSAFE),
            mock.call(self.zk.ensure_path, '/foo/bar/baz/abc/test')]
        self.zk.retry.assert_has_calls(calls)

    def test_delete_node(self):
        self.reg._delete_node()
        self.zk.retry.assert_called_once_with(
            self.zk.delete, '/unittest/host:22')

    def test_delete_node_noauth(self):
        self.zk.retry.side_effect = exceptions.NoAuthError('Boom!')
        self.reg._delete_node()
        self.zk.retry.assert_called_once_with(
            self.zk.delete, '/unittest/host:22')

    def test_delete_node_misc_exc(self):
        self.zk.retry.side_effect = TestExc('Oh snap!')
        self.reg._delete_node()
        self.zk.retry.assert_called_once_with(
            self.zk.delete, '/unittest/host:22')

    def test_update_data(self):
        self.reg._update_data()
        self.zk.retry.assert_called_once_with(
            self.zk.set, '/unittest/host:22', value=self.reg._encoded_data)

    def test_update_data_node_noauth(self):
        self.zk.retry.side_effect = exceptions.NoAuthError('Boom!')
        self.reg._update_data()
        self.zk.retry.assert_called_once_with(
            self.zk.set, '/unittest/host:22', value=self.reg._encoded_data)

    def test_update_data_misc_exc(self):
        self.zk.retry.side_effect = TestExc('Oh snap!')
        self.reg._update_data()
        self.zk.retry.assert_called_once_with(
            self.zk.set, '/unittest/host:22', value=self.reg._encoded_data)

    @patch.object(registration.RegistrationBase, '_create_node')
    @patch.object(registration.RegistrationBase, '_delete_node')
    def test_update_state(self, mock_delete, mock_create):
        self.reg._update_state(True)
        self.reg._update_state(False)
        mock_create.assert_called_once_with()
        mock_delete.assert_called_once_with()


class DataNodeTests(unittest.TestCase):

    # A flag for filtering nose tests
    unit = True

    @mock.patch('kazoo.recipe.watchers.DataWatch')
    @mock.patch('kazoo.recipe.watchers.ChildrenWatch')
    @patch.object(watcher.Watcher, 'add_callback')
    def test_data_node(self,
                       mock_method,
                       mock_children_watch,
                       mock_data_watch):

        zk = mock.MagicMock()
        zk.handler.lock_object.return_value = threading.Lock()

        data = {'hello': 'world'}
        node = registration.DataNode(zk, '/service/path', data)
        self.assertEqual(node._data, data)
        self.assertFalse(node._ephemeral)
        mock_method.assert_called_once_with(node._update)

    @mock.patch('kazoo.recipe.watchers.DataWatch')
    @mock.patch('kazoo.recipe.watchers.ChildrenWatch')
    @patch.object(watcher.Watcher, 'add_callback')
    def test_update(self, mock_method, mock_children_watch, mock_data_watch):
        zk = mock.MagicMock()
        zk.handler.lock_object.return_value = threading.Lock()

        initial_data = {'hello': 'world'}
        node = registration.DataNode(zk, '/service/path', initial_data)

        # Now, execute the _update() method with some new data and make
        # sure that the object updates itself properly.
        new_data = {'children': {},
                    'data': {"foo": "bar", "pid": "1234", "created": "test"},
                    'path': '/service/path',
                    'stat': None}
        node._update(new_data)
        self.assertEquals({"foo": "bar"}, node._data)
        self.assertTrue(b'pid' in node._encoded_data)
        self.assertTrue(b'created' in node._encoded_data)
        self.assertEquals(new_data['data'], node._decoded_data)


class EphemeralNodeTests(unittest.TestCase):

    # A flag for filtering nose tests
    unit = True

    @mock.patch('kazoo.recipe.watchers.DataWatch')
    @mock.patch('kazoo.recipe.watchers.ChildrenWatch')
    @patch.object(watcher.Watcher, 'add_callback')
    def test_ephemeral_node(self,
                            mock_method,
                            mock_children_watch,
                            mock_data_watch):

        zk = mock.MagicMock()
        zk.handler.lock_object.return_value = threading.Lock()

        data = {'hello': 'world'}
        node = registration.EphemeralNode(zk, '/service/path', data)
        self.assertEqual(node._data, data)
        self.assertTrue(node._ephemeral)
        mock_method.assert_called_once_with(node._update)
