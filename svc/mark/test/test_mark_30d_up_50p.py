import unittest

import pandas as pd

from svc.mark.mark_30d_up_50p import mark_30d_up_50p


class TestMark30dUp50p(unittest.TestCase):

    def test_mark(self):
        mark_30d_up_50p.mark(ts_code='000001.SZ')

    def test_rolling(self):
        data = pd.DataFrame({
            'close': [1, 2, 3, 4, 5]
        })
        data['d2_max'] = data['close'].rolling(window=2).max()
        self.assertListEqual(data['d2_max'].fillna(0).to_list(), [0.0, 2.0, 3.0, 4.0, 5.0])

        data['d2_min'] = data['close'].rolling(window=2).min()
        self.assertListEqual(data['d2_min'].fillna(0).to_list(), [0.0, 1.0, 2.0, 3.0, 4.0])

        self.assertListEqual(data['d2_max'].shift(-2).fillna(0).to_list(), [3.0, 4.0, 5.0, 0.0, 0.0])
