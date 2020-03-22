import datetime
import unittest
from unittest.mock import patch

import pandas as pd

from job.guess_indicators_daily_job import stock_stats_index_calculator as calculator
from lib.mysql import mysql
from model.ss_stock_statistics import ss_stock_statistics


class TestGuessIndicatorsDailyJob(unittest.TestCase):

    @patch('lib.mysql.mysql.insert_db')
    @patch('lib.pandas.pandas.bash_stock_tmp', __file__.replace('.py', '/%s/%s/'))
    @patch('lib.pandas.pandas.create_data_frame_by_sql')
    @patch('job.guess_indicators_daily_job.stock_stats_index_calculator.batch_size', 1)
    @patch('lib.mysql.mysql.count_by_date')
    @patch('lib.mysql.mysql.del_by_date')
    @patch('lib.mysql.mysql.count_with_where_clause')
    def test_stat_all_batch(self,
                            count_with_where_clause,
                            del_by_date,
                            count_by_date,
                            create_data_frame_by_sql,
                            insert_db):
        data = {
            'trade': ['13.42'],
            'code': ['000001'],
            'date': ['20200320'],
        }
        create_data_frame_by_sql.return_value = pd.DataFrame(data)

        count_with_where_clause.return_value = 1
        count_by_date.return_value = 1
        calculator.run(datetime.date(2020, 3, 20))
        self.assertEqual(insert_db.called, True)
        (), kwargs = insert_db.call_args
        index = {'trade': '13.42', 'code': '000001', 'date': '20200320', 'adx': 59.87459509612469,
                 'adxr': 43.47846122619745, 'boll': 14.360499999999998, 'boll_lb': 12.499199096698312,
                 'boll_ub': 16.221800903301684, 'cci': -141.23742761353265, 'cci_20': -187.9178014360005,
                 'close_-1_r': 2.3712183156173294, 'close_-2_r': -1.4948859166011075, 'cr': 59.148264984227104,
                 'cr-ma1': 92.73697681391491, 'cr-ma2': 86.43000572949333, 'cr-ma3': 70.08505306891203,
                 'dma': -1.410999999999989, 'dx': 75.87257759601384, 'kdjd': 16.220011950945512,
                 'kdjj': 11.380454938424982, 'kdjk': 14.606826280105336, 'macd': -0.6521004291149559,
                 'macdh': -0.2405502712229684, 'macds': -0.4115501578919875, 'mdi': 51.668392452420306,
                 'pdi': 7.088229141088842, 'rsi_12': 27.038209011520678, 'rsi_6': 22.028438701377794,
                 'trix': -0.48744444168323703, 'trix_9_sma': -0.26238522279917836, 'vr': 61.83256612156151,
                 'vr_6_sma': 60.3181684411804, 'wr_10': 79.46127946127949, 'wr_6': 77.15355805243448}
        self.assertDictEqual(kwargs['data'].loc[0].to_dict(), index)

    @patch('lib.mysql.mysql.insert_db')
    @patch('lib.pandas.pandas.bash_stock_tmp', __file__.replace('.py', '/%s/%s/'))
    def test_stat_index_all(self, insert_db):
        data = pd.DataFrame({
            'trade': ['13.42'],
            'code': ['000001'],
            'date': ['20200320'],
        })
        data = calculator.append_stock_stats(data)
        ss_stock_statistics.insert(data)

        self.assertEqual(insert_db.called, True)
        (), kwargs = insert_db.call_args
        index = {'trade': '13.42', 'code': '000001', 'date': '20200320', 'adx': 59.87459509612469,
                 'adxr': 43.47846122619745, 'boll': 14.360499999999998, 'boll_lb': 12.499199096698312,
                 'boll_ub': 16.221800903301684, 'cci': -141.23742761353265, 'cci_20': -187.9178014360005,
                 'close_-1_r': 2.3712183156173294, 'close_-2_r': -1.4948859166011075, 'cr': 59.148264984227104,
                 'cr-ma1': 92.73697681391491, 'cr-ma2': 86.43000572949333, 'cr-ma3': 70.08505306891203,
                 'dma': -1.410999999999989, 'dx': 75.87257759601384, 'kdjd': 16.220011950945512,
                 'kdjj': 11.380454938424982, 'kdjk': 14.606826280105336, 'macd': -0.6521004291149559,
                 'macdh': -0.2405502712229684, 'macds': -0.4115501578919875, 'mdi': 51.668392452420306,
                 'pdi': 7.088229141088842, 'rsi_12': 27.038209011520678, 'rsi_6': 22.028438701377794,
                 'trix': -0.48744444168323703, 'trix_9_sma': -0.26238522279917836, 'vr': 61.83256612156151,
                 'vr_6_sma': 60.3181684411804, 'wr_10': 79.46127946127949, 'wr_6': 77.15355805243448}
        self.assertAlmostEqual(kwargs['data'].loc[0].to_dict(), index)

    @patch('lib.pandas.pandas.bash_stock_tmp', __file__.replace('.py', '/%s/%s/'))
    def test_concat_guess_data(self):
        data = pd.DataFrame({
            'trade': ['13.42'],
            'code': ['000001'],
            'date': ['20200320'],
        })
        result = calculator.append_stock_stats(data)

        index = {'trade': '13.42', 'code': '000001', 'date': '20200320', 'adx': 59.87459509612469,
                 'adxr': 43.47846122619745, 'boll': 14.360499999999998, 'boll_lb': 12.499199096698312,
                 'boll_ub': 16.221800903301684, 'cci': -141.23742761353265, 'cci_20': -187.9178014360005,
                 'close_-1_r': 2.3712183156173294, 'close_-2_r': -1.4948859166011075, 'cr': 59.148264984227104,
                 'cr-ma1': 92.73697681391491, 'cr-ma2': 86.43000572949333, 'cr-ma3': 70.08505306891203,
                 'dma': -1.410999999999989, 'dx': 75.87257759601384, 'kdjd': 16.220011950945512,
                 'kdjj': 11.380454938424982, 'kdjk': 14.606826280105336, 'macd': -0.6521004291149559,
                 'macdh': -0.2405502712229684, 'macds': -0.4115501578919875, 'mdi': 51.668392452420306,
                 'pdi': 7.088229141088842, 'rsi_12': 27.038209011520678, 'rsi_6': 22.028438701377794,
                 'trix': -0.48744444168323703, 'trix_9_sma': -0.26238522279917836, 'vr': 61.83256612156151,
                 'vr_6_sma': 60.3181684411804, 'wr_10': 79.46127946127949, 'wr_6': 77.15355805243448}
        self.assertAlmostEqual(result.loc[0].to_dict(), index)

    def test_count_table_by_day(self):
        date = datetime.date(1888, 1, 1)
        self.assertEqual(mysql.count_by_date('guess_indicators_daily', date), 0)


class TestDateMethod(unittest.TestCase):

    def test_create_date(self):
        day = datetime.date(2020, 2, 1)
        self.assertEqual(day.strftime("%Y-%m-%d"), '2020-02-01')
