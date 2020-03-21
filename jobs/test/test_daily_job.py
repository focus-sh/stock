import unittest
from unittest.mock import patch


class TestDailyJob(unittest.TestCase):

    @patch('libs.pandas.pandas.bash_stock_tmp', __file__.replace('.py', '/%s/%s/'))
    def test_run(self):
        pass
