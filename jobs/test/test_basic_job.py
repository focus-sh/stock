import unittest

from jobs.basic_job import stat_all


class BasicJobTestSuites(unittest.TestCase):

    def test_can_use_unittest_frame(self):
        self.assertEqual(1+1, 2)

    @staticmethod
    def test_can_run_stat_all_job():
        stat_all('Anything')  # should not raise


if __name__ == '__main__':
    unittest.main()
