import unittest
from unittest.mock import patch

import pandas as pd

from lib.file.tensorflow_total import file_system


class TestTensorflowTotal(unittest.TestCase):

    @patch('lib.file.tensorflow_total.file_system.base_dir', __file__.replace('.py', ''))
    def test_temsorflow_total(self):
        file_system.init_root()
        data = pd.DataFrame(
            {
                'x0': [0.01, 0.02, 0.03],
                'x1': [1, 234, 124.5],
                'x3': [0.03, 0.05, 0.1],
                'y': [1, 0, 1]
            }
        )

        # FILE NAME:
        # training.train.gzip.pickle
        # testing.train.gzip.pickle
        # |x1|x2|...|xn|y|
        file_name = 'training.stock.gzip.pickle'
        file_system.write(data, file_name=file_name)

        result = file_system.read(file_name=file_name)
        self.assertDictEqual(data.to_dict(), result.to_dict())

        file_system.clear()
