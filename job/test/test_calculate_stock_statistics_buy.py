import datetime
import unittest
from unittest.mock import patch
import pandas as pd

from job.calculate_stock_statistics_buy import calculate_stock_statistics_buy


class TestCalculateStockStatisticsBuy(unittest.TestCase):
    # @unittest.skip
    def test_run_0(self):
        calculate_stock_statistics_buy.run(datetime.date(2019, 2, 11))

    @patch('model.stock_statistics_buy.stock_statistics_buy.insert')
    @patch('model.stock_statistics_lite.stock_statistics_lite.select')
    @patch('lib.pandas.pandas.bash_stock_tmp', __file__.replace('.py', '/%s/%s/'))
    def test_run(self, stock_statistics_lite_select, stock_statistics_buy_insert):
        stock_statistics_lite_select.return_value = pd.DataFrame(
            {
                'date': {0: '20190211', 1: '20190211', 2: '20190211', 3: '20190211', 4: '20190211'},
                'code': {0: '000877', 1: '002090', 2: '601229', 3: '603586', 4: '603668'},
                'name': {0: '天山股份', 1: '金智科技', 2: '上海银行', 3: '金麒麟', 4: '天马科技'},
                'changepercent': {0: 0.875, 1: 0.371, 2: 0.123, 3: 0.68, 4: 4.864},
                'trade': {0: 11.53, 1: 10.81, 2: 8.15, 3: 16.28, 4: 9.27},
                'turnoverratio': {0: 3.791, 1: 9.1717, 2: 0.1265, 3: 1.4034, 4: 6.6264},
                'pb': {0: 1.253, 1: 3.349, 2: 0.755, 3: 1.556, 4: 2.757},
                'kdjj': {0: 102.535, 1: 107.3322, 2: 95.598, 3: 105.64, 4: 102.8839},
                'rsi_6': {0: 82.0646, 1: 81.1132, 2: 69.3309, 3: 73.6427, 4: 76.5764},
                'cci': {0: 199.7164, 1: 156.0914, 2: 124.4655, 3: 220.2424, 4: 241.8554}
            }
        )

        index = {
            'buy_date': {3: '20190211', 4: '20190211'},
            'code': {3: '603586', 4: '603668'},
            'name': {3: '金麒麟', 4: '天马科技'},
            'changepercent': {3: 0.68, 4: 4.864},
            'trade': {3: 16.28, 4: 9.27},
            'turnoverratio': {3: 1.4034, 4: 6.6264},
            'pb': {3: 1.556, 4: 2.757},
            'kdjj': {3: 105.64, 4: 102.8839},
            'rsi_6': {3: 73.6427, 4: 76.5764},
            'cci': {3: 220.2424, 4: 241.8554},
            'wave_mean': {3: 14.767587939698492, 4: 8.71251256281407},
            'wave_crest': {3: 20.72, 4: 11.556666666666667},
            'wave_base': {3: 11.043333333333331, 4: 7.19},
            'up_rate': {3: -7.299286005316164, 4: -4.823946673082753},
            'buy': {3: 1, 4: 1}, 'sell': {3: 0, 4: 0},
            'today_trade': {3: 16.28, 4: 9.27},
            'income': {3: 0, 4: 0}
        }

        calculate_stock_statistics_buy.run(datetime.date(2019, 2, 11))
        [statistics], _ = stock_statistics_buy_insert.call_args
        self.assertDictEqual(statistics.to_dict(), index)

    @patch('lib.pandas.pandas.bash_stock_tmp', __file__.replace('.py', '/%s/%s/'))
    def test_calculate_statistics_when_stock_none(self):
        index = {
            'date': '20190211',
            'code': '002949',
            'wave_mean': 0,
            'wave_crest': 0,
            'wave_base': 0
        }
        result = calculate_stock_statistics_buy.calculate_statistics(code='002949', date='20190211')
        self.assertDictEqual(result.to_dict(), index)

