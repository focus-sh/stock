import unittest
from unittest.mock import patch

from lib.stockstats import stockstats


class TestStockStats(unittest.TestCase):

    @patch('lib.pandas.pandas.bash_stock_tmp', __file__.replace('.py', '/%s/%s/'))
    def test_calculate_statistics(self):
        index = {
            'adx': 59.87459509612469,
            'adxr': 43.47846122619745,
            'boll': 14.360499999999998,
            'boll_lb': 12.499199096698312,
            'boll_ub': 16.221800903301684,
            'cci': -141.23742761353265,
            'cci_20': -187.9178014360005,
            'close_-1_r': 2.3712183156173294,
            'close_-2_r': -1.4948859166011075,
            'code': '000001',
            'cr': 59.148264984227104,
            'cr-ma1': 92.73697681391491,
            'cr-ma2': 86.43000572949333,
            'cr-ma3': 70.08505306891203,
            'date': '20200320',
            'dma': -1.410999999999989,
            'dx': 75.87257759601384,
            'kdjd': 16.220011950945512,
            'kdjj': 11.380454938424982,
            'kdjk': 14.606826280105336,
            'macd': -0.6521004291149559,
            'macdh': -0.2405502712229684,
            'macds': -0.4115501578919875,
            'mdi': 51.668392452420306,
            'pdi': 7.088229141088842,
            'rsi_12': 27.038209011520678,
            'rsi_6': 22.028438701377794,
            'trix': -0.48744444168323703,
            'trix_9_sma': -0.26238522279917836,
            'vr': 61.83256612156151,
            'vr_6_sma': 60.3181684411804,
            'wr_10': 79.46127946127949,
            'wr_6': 77.15355805243448
        }
        result = stockstats.calculate_statistics(date='20200320', code='000001')
        self.assertDictEqual(result.to_dict(), index)

    @patch('lib.pandas.pandas.bash_stock_tmp', __file__.replace('.py', '/%s/%s/'))
    def test_calculate_statistics_with_no_his_data(self):
        index = {
            'code': '002949',
            'date': '20190211',
            'adx': 0,
            'adxr': 0,
            'boll': 0,
            'boll_lb': 0,
            'boll_ub': 0,
            'cci': 0,
            'cci_20': 0,
            'close_-1_r': 0,
            'close_-2_r': 0,
            'cr': 0,
            'cr-ma1': 0,
            'cr-ma2': 0,
            'cr-ma3': 0,
            'dma': 0,
            'dx': 0,
            'kdjd': 0,
            'kdjj': 0,
            'kdjk': 0,
            'macd': 0,
            'macdh': 0,
            'macds': 0,
            'mdi': 0,
            'pdi': 0,
            'rsi_12': 0,
            'rsi_6': 0,
            'trix': 0,
            'trix_9_sma': 0,
            'vr': 0,
            'vr_6_sma': 0,
            'wr_10': 0,
            'wr_6': 0
        }
        result = stockstats.calculate_statistics(code='002949', date='20190211')
        self.assertDictEqual(result.to_dict(), index)

    @patch('lib.pandas.pandas.bash_stock_tmp', __file__.replace('.py', '/%s/%s/'))
    def test_calculate_wave_when_stock_none(self):
        index = {
            'date': '20190211',
            'code': '002949',
            'wave_mean': 0,
            'wave_crest': 0,
            'wave_base': 0
        }
        result = stockstats.calculate_wave(code='002949', date='20190211')
        self.assertDictEqual(result.to_dict(), index)
