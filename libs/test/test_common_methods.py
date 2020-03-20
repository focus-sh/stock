import shutil
import unittest
from unittest.mock import patch

from libs.common import format_value, get_hist_data_cache


class TestCommonMethods(unittest.TestCase):

    def test_can_format_float_num(self):
        val = format_value('0.2341242523535')
        self.assertEqual(val, '0.2341')

    def test_can_format_None(self):
        self.assertIsNone(format_value(None))

    def test_can_format_str(self):
        self.assertEqual(format_value("hello world"), "hello world")

    @patch('libs.common.bash_stock_tmp', './data/%s/%s/')
    def test_get_hist_data_cache(self):
        try:
            stock = get_hist_data_cache('000001', '2020-01-01', '2020-03-01')
            self.assertIsNotNone(stock)
        finally:
            pass
            shutil.rmtree('./data')

