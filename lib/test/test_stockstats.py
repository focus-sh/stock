import unittest
from unittest.mock import patch

from lib.stockstats import stockstats


class TestStockStats(unittest.TestCase):

    @patch('lib.pandas.pandas.bash_stock_tmp', __file__.replace('.py', '/%s/%s/'))
    def test_calculate_statistics(self):
        index = {'adx': 59.87459509612469, 'adxr': 43.47846122619745, 'boll': 14.360499999999998,
                 'boll_lb': 12.499199096698312, 'boll_ub': 16.221800903301684, 'cci': -141.23742761353265,
                 'cci_20': -187.9178014360005, 'close_-1_r': 2.3712183156173294, 'close_-2_r': -1.4948859166011075,
                 'code': '000001', 'cr': 59.148264984227104, 'cr-ma1': 92.73697681391491, 'cr-ma2': 86.43000572949333,
                 'cr-ma3': 70.08505306891203, 'date': '20200320', 'dma': -1.410999999999989, 'dx': 75.87257759601384,
                 'kdjd': 16.220011950945512, 'kdjj': 11.380454938424982, 'kdjk': 14.606826280105336,
                 'macd': -0.6521004291149559, 'macdh': -0.2405502712229684, 'macds': -0.4115501578919875,
                 'mdi': 51.668392452420306, 'pdi': 7.088229141088842, 'rsi_12': 27.038209011520678,
                 'rsi_6': 22.028438701377794, 'trix': -0.48744444168323703, 'trix_9_sma': -0.26238522279917836,
                 'vr': 61.83256612156151, 'vr_6_sma': 60.3181684411804, 'wr_10': 79.46127946127949,
                 'wr_6': 77.15355805243448}
        self.assertAlmostEqual(stockstats.calculate_statistics(date='20200320', code='000001').to_dict(), index)