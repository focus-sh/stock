import unittest
from datetime import date, timedelta

from jobs.guess_indicators_lite_sell_daily_job import stat_index_calculate


class TestGuessIndicatorsLiteSellDailyJob(unittest.TestCase):
    def setUp(self):
        self.day = 1

    def test_stat_all_lite(self):
        stat_index_calculate(date.today() - timedelta(self.day))  # should not raise