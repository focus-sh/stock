import unittest
from datetime import date

from jobs.daily_job import stat_all


class DailyJobTestSuites(unittest.TestCase):

    def test_can_use_unittest_frame(self):
        self.assertEqual(1+1, 2)

    @staticmethod
    def test_can_run_stat_all_job():
        stat_all(date.fromisoformat('2020-03-13'))  # should not raise


if __name__ == '__main__':
    unittest.main()
