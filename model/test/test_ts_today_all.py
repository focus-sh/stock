import datetime
import unittest

from model.ts_today_all import ts_today_all


class TestTsTodayAll(unittest.TestCase):
    @unittest.skip('Need database')
    def test_count(self):
        date = datetime.date(1988, 3, 20)
        count = ts_today_all.count(date)
        self.assertEqual(count, 0)
