import unittest

from jobs.basic_job import stat_all


class TestBasicJob(unittest.TestCase):

    @staticmethod
    def test_can_run_stat_all_job():
        stat_all('Anything')  # should not raise
