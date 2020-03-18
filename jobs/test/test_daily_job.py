import unittest
from datetime import date, timedelta

from jobs.daily_job import stat_all


class TestDailyJob(unittest.TestCase):

    def test_can_run_stat_all_job(self):
        stat_all(date.today() - timedelta(days=1))  # should not raise
