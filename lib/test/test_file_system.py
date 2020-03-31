import logging
import os
import pandas as pd
import unittest
from unittest.mock import patch

from lib.file_system import file_system


class TestFileSystem(unittest.TestCase):
    def setUp(self):
        # TODO 日志设置需要重写
        logging.basicConfig(level=logging.NOTSET)

    @patch('lib.file_system.file_system.base_dir', __file__.replace('.py', ''))
    def test_clear(self):
        base_dir = __file__.replace('.py', '')
        os.makedirs(base_dir, exist_ok=True)
        sub_dir = base_dir + '/sub_dir'
        os.makedirs(sub_dir, exist_ok=True)
        file_system.clear()
        self.assertTrue(os.path.exists(base_dir))
        self.assertFalse(os.path.exists(sub_dir))

    @patch('lib.file_system.file_system.base_dir', __file__.replace('.py', ''))
    def test_create_dir(self):
        base_dir = __file__.replace('.py', '')
        relative_path = 'sub_dir'
        file_system.create_dir(relative_path)
        self.assertTrue(os.path.exists(base_dir + '/' + relative_path))
        file_system.clear()
        self.assertFalse(os.path.exists(base_dir + '/' + relative_path))

    @patch('lib.file_system.file_system.get_file_name')
    @patch('lib.file_system.file_system.base_dir', __file__.replace('.py', ''))
    def test_write(self, get_file_name):
        base_dir = __file__.replace('.py', '')
        get_file_name.return_value = base_dir + '/ts_code_write_test.stock'

        file_system.clear()

        data = pd.DataFrame(
            {
                'ts_code': ['000001.SZ']
            }
        )
        file_system.write(data, name='test.stock')
        self.assertTrue(get_file_name.called)
        _, kwargs = get_file_name.call_args
        self.assertEqual(kwargs['name'], 'test.stock')

        # Read the saved data
        result = file_system.read(name='test.stock')
        self.assertTrue(get_file_name.called)
        _, kwargs = get_file_name.call_args
        self.assertEqual(kwargs['name'], 'test.stock')

        # Compare if is equal
        self.assertDictEqual(result.to_dict(), data.to_dict())

        # Clean
        file_system.clear()

    @patch('lib.file_system.file_system.get_file_name')
    @patch('lib.file_system.file_system.base_dir', __file__.replace('.py', ''))
    def test_delete(self, get_file_name):
        base_dir = __file__.replace('.py', '')
        get_file_name.return_value = base_dir + '/ts_code_delete_test.stock'

        file_system.clear()

        # Write a file
        data = pd.DataFrame(
            {
                'ts_code': ['000001.SZ']
            }
        )
        file_system.write(data, name='test.stock')

        # Delete it
        file_system.delete(name='test.stock')
        self.assertFalse(os.path.exists(get_file_name.return_value))

        # Clean
        file_system.clear()

    @patch('lib.file_system.file_system.get_file_name')
    @patch('lib.file_system.file_system.base_dir', __file__.replace('.py', ''))
    def test_read(self, get_file_name):
        base_dir = __file__.replace('.py', '')
        get_file_name.return_value = base_dir + '/ts_code_read_test.stock'

        file_system.clear()

        # Read a not exist file
        result = file_system.read(name='test_stock')
        self.assertIsNone(result)

        # Write data for reading.
        data = pd.DataFrame(
            {
                'ts_code': ['000001.SZ']
            }
        )
        file_system.write(data, name='test.stock')

        # Read again
        result = file_system.read(name='test.stock')

        self.assertTrue(get_file_name.called)
        _, kwargs = get_file_name.call_args
        self.assertEqual(kwargs['name'], 'test.stock')

        # Compare if is equal
        self.assertDictEqual(result.to_dict(), data.to_dict())

        file_system.clear()
