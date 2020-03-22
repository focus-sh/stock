import unittest

from lib.tushare import tushare


class TestTushare(unittest.TestCase):
    def test_should_drop_duplicates(self):
        self.assertTrue(tushare.should_drop_duplicates(None, None))
        self.assertTrue(tushare.should_drop_duplicates(None, []))
        self.assertFalse(tushare.should_drop_duplicates("code", "code"))
        self.assertFalse(tushare.should_drop_duplicates("code", ["code", "date"]))
        self.assertTrue(tushare.should_drop_duplicates("code", ["id", "man"]))
