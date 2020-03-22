import unittest

from lib.mysql import mysql


class TestMySql(unittest.TestCase):

    def test_concat_list_params(self):
        arr = ['a', 'b', 'c', 'd']
        self.assertEqual(mysql.concat_list_params(arr), '`a`, `b`, `c`, `d`')

        arr = 'code'
        self.assertEqual(mysql.concat_list_params(arr), '`code`')
