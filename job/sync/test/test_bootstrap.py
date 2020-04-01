import unittest

import pandas as pd

from job.sync.bootstrap import bootstrap


class TestDownloadProBar(unittest.TestCase):

    def test_calculate_start_date_input_None(self):
        start_date = bootstrap.calculate_start_date(None, None)
        self.assertIsNone(start_date)

        start_date = bootstrap.calculate_start_date(pd.DataFrame({}), None)
        self.assertIsNone(start_date)

        start_date = bootstrap.calculate_start_date(pd.DataFrame({'ts_code': ['000001.SZ']}), None)
        self.assertIsNone(start_date)

        start_date = bootstrap.calculate_start_date(pd.DataFrame({'trade_date': []}), None)
        self.assertEqual(start_date, None)

        start_date = bootstrap.calculate_start_date(None, '20081012')
        self.assertEqual(start_date, '20081012')

        start_date = bootstrap.calculate_start_date(pd.DataFrame({}), '20081012')
        self.assertEqual(start_date, '20081012')

        start_date = bootstrap.calculate_start_date(pd.DataFrame({'ts_code': ['000001.SZ']}), '20081012')
        self.assertEqual(start_date, '20081012')

        start_date = bootstrap.calculate_start_date(pd.DataFrame({'trade_date': ['20100101']}), None)
        self.assertEqual(start_date, '20100102')

    def test_calculate_start_date(self):
        stock = pd.DataFrame(
            {
                'ts_code': ['000001.SZ'],
                'trade_date': ['20000302']
            }
        )
        list_date = '20000401'
        start_date = bootstrap.calculate_start_date(stock, list_date)
        self.assertEqual(start_date, '20000401')

        list_date = '20000201'
        start_date = bootstrap.calculate_start_date(stock, list_date)
        self.assertEqual(start_date, '20000303')


