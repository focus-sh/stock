import datetime
import unittest
from unittest.mock import patch

import pandas as pd

from job.calculate_period_return_index import calculate_period_return_index


class TestCalculatePeriodReturnIndex(unittest.TestCase):

    @patch('model.period_return_index.period_return_index.insert')
    @patch('model.period_return_index.period_return_index.delete')
    @patch('model.ts_today_all.ts_today_all.select')
    @patch('lib.pandas.pandas.bash_stock_tmp', __file__.replace('.py', '/%s/%s/'))
    def test_stat_run(self, ts_today_all_select, ts_return_index_delete, ts_return_index_insert):
        ts_today_all_select.return_value = pd.DataFrame({
            'date': {0: '20200326', 1: '20200326'},
            'code': {0: '000001', 1: '000005'},
            'name': {0: '平安银行', 1: '世纪星源'},
            'changepercent': {0: 2.379, 1: 1.064},
            'trade': {0: 12.91, 1: 2.85},
            'open': {0: 12.88, 1: 2.87},
            'high': {0: 13.07, 1: 2.89},
            'low': {0: 12.87, 1: 2.83},
            'settlement': {0: 12.61, 1: 2.82},
            'volume': {0: 52390438.0, 1: 3039905.0},
            'turnoverratio': {0: 0.27, 1: 0.2873},
            'amount': {0: 677607194.0, 1: 8681277.0},
            'per': {0: 8.383, 1: 20.27},
            'pb': {0: 0.918, 1: 1.963},
            'mktcap': {0: 25053040.3936, 1: 301683.0},
            'nmc': {0: 25052826.7099, 1: 301514.679}
        })

        index = {
            'date': {0: '20200326', 1: '20200326'},
            'code': {0: '000001', 1: '000005'},
            'name': {0: '平安银行', 1: '世纪星源'},
            'changepercent': {0: 2.379, 1: 1.064},
            'trade': {0: 12.91, 1: 2.85},
            'open': {0: 12.88, 1: 2.87},
            'high': {0: 13.07, 1: 2.89},
            'low': {0: 12.87, 1: 2.83},
            'settlement': {0: 12.61, 1: 2.82},
            'volume': {0: 52390438.0, 1: 3039905.0},
            'turnoverratio': {0: 0.27, 1: 0.2873},
            'amount': {0: 677607194.0, 1: 8681277.0},
            'per': {0: 8.383, 1: 20.27},
            'pb': {0: 0.918, 1: 1.963},
            'mktcap': {0: 25053040.3936, 1: 301683.0},
            'nmc': {0: 25052826.7099, 1: 301514.679},
            '10d': {0: 13.145000000000021, 1: 2.8679999999999986},
            '20d': {0: 13.978999999999996, 1: 2.9300000000000024},
            '5-10d': {0: -5.089387599847927, 1: -1.324965132496368},
            '5-20d': {0: -10.751842048787369, 1: -3.4129692832764356},
            '5d': {0: 12.47600000000001, 1: 2.8300000000000027},
            '60d': {0: 15.207833333333339, 1: 2.969166666666666},
            'mov_vol': {0: 10.311674957947844, 1: 6.887144907856321},
            'return': {0: 2.0408871631207033, 1: 2.1053409197832265}
        }

        calculate_period_return_index.run(datetime.date(2020, 3, 26))

        self.assertTrue(ts_return_index_delete.called)
        self.assertTrue(ts_return_index_insert.called)

        [statistics], _ = ts_return_index_insert.call_args
        self.assertDictEqual(statistics.to_dict(), index)

    @patch('lib.pandas.pandas.bash_stock_tmp', __file__.replace('.py', '/%s/%s/'))
    def test_calculate_statistics_when_stock_none(self):
        index = {
            'date': '20190211',
            'code': '002949',
            '10d': 0.0,
            '20d': 0.0,
            '5-10d': 0.0,
            '5-20d': 0.0,
            '5d': 0.0,
            '60d': 0.0,
            'mov_vol': 0.0,
            'return': 0.0
        }
        result = calculate_period_return_index.calculate_return(code='002949', date='20190211')
        self.assertDictEqual(result.to_dict(), index)
