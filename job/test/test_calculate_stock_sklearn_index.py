import datetime
import unittest
from unittest.mock import patch

import pandas as pd

from job.calculate_stock_sklearn_index import calculate_stock_sklearn_index


class TestCalculateStockSklearnIndex(unittest.TestCase):

    @patch('model.stock_sklearn_index.stock_sklearn_index.insert')
    @patch('model.ts_today_all.ts_today_all.paged_select')
    @patch('model.ts_today_all.ts_today_all.count')
    @patch('model.stock_sklearn_index.stock_sklearn_index.delete')
    @patch('lib.pandas.pandas.bash_stock_tmp', __file__.replace('.py', '/%s/%s/'))
    @patch('job.calculate_stock_sklearn_index.calculate_stock_sklearn_index.batch_size', 1)
    def test_run(
            self,
            stock_sklearn_index_delete,
            ts_today_all_count,
            ts_today_all_paged_select,
            stock_sklearn_index_insert
    ):
        ts_today_all_count.return_value = 1
        data = {
            'date': {0: '20200326'},
            'code': {0: '000001'},
            'name': {0: '平安银行'},
            'changepercent': {0: 2.062},
            'trade': {0: 12.87},
            'open': {0: 12.88},
            'high': {0: 13.07},
            'low': {0: 12.7},
            'settlement': {0: 12.61},
            'volume': {0: 113695774.0},
            'turnoverratio': {0: 0.5859},
            'amount': {0: 1467534956.0},
            'per': {0: 8.357},
            'pb': {0: 0.915},
            'mktcap': {0: 24975416.7208},
            'nmc': {0: 24975203.6992}
        }
        ts_today_all_paged_select.return_value = pd.DataFrame(data)

        calculate_stock_sklearn_index.run(datetime.date(2020, 3, 26))
        index = {
            'date': {0: '20200326'},
            'code': {0: '000001'},
            'name': {0: '平安银行'},
            'changepercent': {0: 2.062},
            'trade': {0: 12.87},
            'open': {0: 12.88},
            'high': {0: 13.07},
            'low': {0: 12.7},
            'settlement': {0: 12.61},
            'volume': {0: 113695774.0},
            'turnoverratio': {0: 0.5859},
            'amount': {0: 1467534956.0},
            'per': {0: 8.357},
            'pb': {0: 0.915},
            'mktcap': {0: 24975416.7208},
            'nmc': {0: 24975203.6992},
            'next_close': {0: 12.975245197581323},
            'sklearn_score': {0: 95.32467272359013},
            'up_rate': {0: 0.8177560029628888}
        }

        self.assertTrue(stock_sklearn_index_insert.called)
        [statistics], _ = stock_sklearn_index_insert.call_args
        self.assertDictEqual(statistics.to_dict(), index)

    @patch('lib.pandas.pandas.bash_stock_tmp', __file__.replace('.py', '/%s/%s/'))
    def test_linear_regression_when_stock_none(self):
        index = {
            'code': '002949',
            'date': '20190211',
            'next_close': 0.0,
            'sklearn_score': 0.0
        }
        result = calculate_stock_sklearn_index.linear_regression(code='002949', date='20190211')
        self.assertDictEqual(result.to_dict(), index)
