import unittest

from lib.datetime import datetime


class TestDatetime(unittest.TestCase):

    def test_next_day(self):
        date_str = None
        self.assertEqual(datetime.next_day(date_str), None)

        date_str = '20200331'
        self.assertEqual(datetime.next_day(date_str), '20200401')

    def test_max(self):
        self.assertIsNone(datetime.max())
        self.assertEqual(datetime.max('20010101'), '20010101')
