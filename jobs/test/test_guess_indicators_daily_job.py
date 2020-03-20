import datetime
import unittest
from unittest.mock import patch

from jobs.guess_indicators_daily_job import stat_all_batch
from libs.common import DataHandler
import pandas as pd


class TestGuessIndicatorsDailyJob(unittest.TestCase):

    @patch('libs.common.insert_db')
    @patch('libs.common.bash_stock_tmp', './data/guess/%s/%s/')
    @patch('libs.common.DataHandler.create_data_frame_by_sql')
    @patch('jobs.guess_indicators_daily_job.batch_size', 1)
    @patch('libs.common.DataHandler.count_by_daystr')
    @patch('libs.common.DataHandler.del_by_daystr')
    @patch('libs.common.DataHandler.count_with_where_clause')
    def test_stat_all_batch(self, count_with_where_clause, del_by_daystr, count_by_daystr, create_data_frame_by_sql,
                            insert_db):
        data = {
            'trade': ['13.42'],
            'code': ['000001'],
            'date': ['20200320'],
        }
        create_data_frame_by_sql.return_value = pd.DataFrame(data)

        count_with_where_clause.return_value = 1
        count_by_daystr.return_value = 1
        stat_all_batch(datetime.date(2020, 3, 20))
        self.assertEqual(insert_db.called, True)
        (data, table_name, write_index, primary_keys), kwargs = insert_db.call_args
        dict = {'trade': '13.42', 'code': '000001', 'date': '20200320', 'adx': 53.48, 'adxr': 36.92, 'boll': 14.51,
                'boll_lb': 12.79, 'boll_ub': 16.24, 'cci': -210.41, 'cci_20': -266.67, 'close_-1_r': -3.78,
                'close_-2_r': -8.8, 'cr': 55.78, 'cr-ma1': 99.66, 'cr-ma2': 83.98, 'cr-ma3': 69.13, 'dma': -1.25,
                'dx': 75.87, 'kdjd': 17.03, 'kdjj': 0.87, 'kdjk': 11.64, 'macd': -0.59, 'macdh': -0.24, 'macds': -0.35,
                'mdi': 58.77, 'pdi': 8.06, 'rsi_12': 21.38, 'rsi_6': 10.77, 'trix': -0.4, 'trix_9_sma': -0.23,
                'vr': 51.6, 'vr_6_sma': 62.82, 'wr_10': 90.48, 'wr_6': 89.08}
        self.assertDictEqual(data.loc[0].to_dict(), dict)

    def test_del_guess_indicators_daily_by_day(self):
        day = datetime.date(2020, 3, 20)
        DataHandler.del_by_daystr('guess_indicators_daily', day)
        self.assertEqual(DataHandler.count_by_daystr('guess_indicators_daily', day), 0)

    def test_count_table_by_day(self):
        day = datetime.date(1888, 1, 1)
        self.assertEqual(DataHandler.count_by_daystr('guess_indicators_daily', day), 0)


class TestDateMethod(unittest.TestCase):

    def test_create_date(self):
        day = datetime.date(2020, 2, 1)
        self.assertEqual(day.strftime("%Y-%m-%d"), '2020-02-01')
