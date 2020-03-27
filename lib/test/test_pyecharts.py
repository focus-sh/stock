import unittest
from pyecharts.charts import Bar
from pyecharts import options as opts
import lib.executor
from model.ts_pro_bar import ts_pro_bar


class TestPyecharts(unittest.TestCase):

    def test_pyecharts(self):
        data = ts_pro_bar.select_all_valid_record('000001.SZ')

        bar = Bar()
        bar.add_xaxis(data['trade_date'].values)
        # line.add_yaxis("开盘价", data['open'].values)
        bar.add_yaxis("收盘价", data['close'].values)
        bar.set_global_opts(title_opts=opts.TitleOpts(title="平安银行"))
        bar.render()
