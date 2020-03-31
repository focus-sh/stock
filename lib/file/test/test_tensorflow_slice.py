import datetime
import unittest
from unittest.mock import patch
import pandas as pd

from lib.file.tensorflow_slice import file_system


class TestTensorflowSlice(unittest.TestCase):

    @patch('lib.file.tensorflow_slice.file_system.base_dir', __file__.replace('.py', ''))
    def test_temsorflow_comm_slice(self):
        date = datetime.date(2018, 3, 31)

        data = pd.DataFrame(
            {
                'ts_code': ['000001.SZ', '000002.SZ', '000003.SZ']
            }
        )

        # FILE NAME:
        # daily_slice.stock.gzip.pickle
        # daily_slice_training.stock.gzip.pickle
        # daily_slice_testing.stock.gzip.pickle
        # |ts_code|
        # daily_slice_training.sample.gzip.pickle
        # daily_slice_testing.sample.gzip.pickle
        # |ts_code|x1|x2|...|xn|y|
        file_name = 'daily_slice.stock.gzip.pickle'
        file_system.write(data, date=date, file_name=file_name)

        result = file_system.read(date=date, file_name=file_name)
        self.assertDictEqual(data.to_dict(), result.to_dict())

        file_system.clear()
