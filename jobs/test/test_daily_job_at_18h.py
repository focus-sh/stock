import unittest
from datetime import date, timedelta
import time

from jobs.daily_job_at_18h import stat_index_all, stat_today_all


class TestDailyJobAt18h(unittest.TestCase):
    def setUp(self):
        self.day = 1

    def test_can_run_stat_index_all(self):
        stat_index_all(date.today() - timedelta(self.day))  # should not raise

    def test_can_run_stat_today_all(self):
        time.sleep(5)
        stat_today_all(date.today() - timedelta(self.day))  # should not raise
