import unittest

from lib.mysql import mysql


class TestMySql(unittest.TestCase):

    def test_should_drop_duplicates(self):
        self.assertTrue(mysql.should_drop_duplicates(None, None))
        self.assertTrue(mysql.should_drop_duplicates(None, []))
        self.assertFalse(mysql.should_drop_duplicates("code", "code"))
        self.assertFalse(mysql.should_drop_duplicates("code", ["code", "date"]))
        self.assertTrue(mysql.should_drop_duplicates("code", ["id", "man"]))
