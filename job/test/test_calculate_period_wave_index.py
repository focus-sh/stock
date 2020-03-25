import datetime
import unittest
from unittest.mock import patch
import pandas as pd
from job.calculate_period_wave_index import calculate_period_wave_index


class TestCalculatePeriodWaveIndex(unittest.TestCase):

    @patch('model.period_wave_index.period_wave_index.insert')
    @patch('model.period_wave_index.period_wave_index.delete')
    @patch('model.ts_today_all.ts_today_all.select')
    @patch('lib.pandas.pandas.bash_stock_tmp', __file__.replace('.py', '/%s/%s/'))
    def test_stat_index_all(self, ts_today_all_select, ts_today_all_delete, ts_today_all_insert):
        ts_today_all_select.return_value = pd.DataFrame({
            'date': {0: '20200325', 1: '20200325'},
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
            'date': {0: '20200325', 1: '20200325'},
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
            'wave_mean': {0: 14.984299999999998, 1: 3.0390999999999995},
            'wave_crest': {0: 17.176, 1: 3.63},
            'wave_base': {0: 11.958, 1: 2.68},
            'up_rate': {0: 12.076734979040507, 1: 5.209366391184556}
        }

        calculate_period_wave_index.run(datetime.date(2020, 3, 25))

        self.assertTrue(ts_today_all_delete.called)
        self.assertTrue(ts_today_all_insert.called)

        [statistics], _ = ts_today_all_insert.call_args
        self.assertDictEqual(statistics.to_dict(), index)