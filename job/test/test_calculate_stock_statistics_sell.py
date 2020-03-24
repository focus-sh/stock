import datetime
import unittest
from unittest.mock import patch
import pandas as pd
from job.calculate_stock_statistics_sell import calculate_stock_statistics_sell


class TestCalculateStockStatisticsSell(unittest.TestCase):

    @patch('model.stock_statistics_sell.stock_statistics_sell.insert')
    @patch('model.stock_statistics_sell.stock_statistics_sell.delete')
    @patch('model.stock_statistics_buy.stock_statistics_buy.select')
    @patch('lib.pandas.pandas.bash_stock_tmp', __file__.replace('.py', '/%s/%s/'))
    def test_run(self, stock_statistics_buy_select, stock_statistics_sell_delete, stock_statistics_sell_insert):
        stock_statistics_buy_select.return_value = pd.DataFrame(
            {
                'buy_date': {0: '20190211', 1: '20190211'},
                'code': {0: '603586', 1: '603668'},
                'name': {0: '金麒麟', 1: '天马科技'},
                'changepercent': {0: 0.68, 1: 4.864},
                'trade': {0: 16.28, 1: 9.27},
                'turnoverratio': {0: 1.4034, 1: 6.6264},
                'pb': {0: 1.556, 1: 2.757},
                'kdjj': {0: 105.64, 1: 102.8839},
                'rsi_6': {0: 73.6427, 1: 76.5764},
                'cci': {0: 220.2424, 1: 241.8554},
                'wave_base': {0: 11.0433, 1: 7.19},
                'wave_crest': {0: 20.72, 1: 11.5567},
                'wave_mean': {0: 14.7676, 1: 8.7125},
                'up_rate': {0: -7.2993, 1: -4.8239}
            }
        )

        index = {
            'buy_date': {0: '20190211', 1: '20190211'},
            'code': {0: '603586', 1: '603668'},
            'name': {0: '金麒麟', 1: '天马科技'},
            'changepercent': {0: 0.68, 1: 4.864},
            'trade': {0: 16.28, 1: 9.27},
            'turnoverratio': {0: 1.4034, 1: 6.6264},
            'pb': {0: 1.556, 1: 2.757},
            'kdjj': {0: 105.64, 1: 102.8839},
            'rsi_6': {0: 73.6427, 1: 76.5764},
            'cci': {0: 220.2424, 1: 241.8554},
            'wave_base': {0: 11.0433, 1: 7.19},
            'wave_crest': {0: 20.72, 1: 11.5567},
            'wave_mean': {0: 14.7676, 1: 8.7125},
            'up_rate': {0: -7.2993, 1: -4.8239},
            'date': {0: '20190324', 1: '20190324'},
            'today_trade': {0: 16.2, 1: 8.92},
            'income': {0: -8.000000000000185, 1: -34.999999999999964},
            'sell': {0: 1, 1: 0}, 'buy': {0: 0, 1: 1},
            'sell_cci': {0: 143.43434343434586, 1: 42.61992619925973},
            'sell_kdjj': {0: 91.51179814381064, 1: 50.96752618157015},
            'sell_rsi_6': {0: 72.52707208882221, 1: 57.06124215284208}
        }

        calculate_stock_statistics_sell.run(datetime.date(2019, 3, 24))
        self.assertTrue(stock_statistics_sell_delete.called)
        self.assertTrue(stock_statistics_sell_insert.called)

        [statistics], _ = stock_statistics_sell_insert.call_args
        self.assertDictEqual(statistics.to_dict(), index)

    @patch('lib.pandas.pandas.bash_stock_tmp', __file__.replace('.py', '/%s/%s/'))
    def test_calculate_statistics_when_stock_none(self):
        index = {
            'code': '002949',
            'date': '20190211',
            'today_trade': 0,
            'sell_cci': 0,
            'sell_kdjj': 0,
            'sell_rsi_6': 0,
            'sell': 0,
            'buy': 1
        }
        result = calculate_stock_statistics_sell.calculate_statistics(code='002949', date='20190211')
        self.assertDictEqual(result.to_dict(), index)
