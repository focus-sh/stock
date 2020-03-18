import unittest
from datetime import date, timedelta

from jobs.guess_indicators_daily_job import stat_all_batch, stat_all_lite


class TestGuessIndicatorsDailyJob(unittest.TestCase):
    def setUp(self):
        self.day = 1

    def test_stat_all_batch(self):
        stat_all_batch(date.today() - timedelta(self.day))  # should not raise

    def test_stat_all_lite(self):
        stat_all_lite(date.today() - timedelta(self.day)) # should not raise
