#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import datetime
import math

import numpy as np
import pandas as pd

from lib.executor import executor
from lib.numpy import numpy
from lib.pandas import pandas
from model.period_return_index import period_return_index
from model.ts_today_all import ts_today_all

"""
SELECT `date`, `code`, `name`, `changepercent`, `trade`, `open`, `high`, `low`, 
                `settlement`, `volume`, `turnoverratio`, `amount`, `per`, `pb`, `mktcap`, `nmc` 
    FROM stock_data.ts_today_all where `date` = 20171106 and trade > 0 and trade <= 20
and `code` not like '002%' and `code` not like '300%'  and `name` not like '%st%'

"""


class CalculatePeriodReturnIndex:
    def run(self, date):
        data = ts_today_all.select(date)

        statistics = data.apply(
            lambda row: self.calculate_return(
                code=row['code'],
                date=row['date'],
            ),
            axis=1
        )
        statistics.drop('date', axis=1, inplace=True)
        statistics = pd.merge(data, statistics, on=['code'], how='left')

        statistics["return"] = statistics["return"].mul(100)
        statistics["mov_vol"] = statistics["mov_vol"].mul(100)

        period_return_index.delete(date)
        period_return_index.insert(statistics)

    @staticmethod
    def calculate_return(code, date):
        stock_name_list = ['date', 'code', '10d', '20d', '5-10d', '5-20d', '5d', '60d', 'mov_vol', 'return']

        stock = pandas.get_stock_hist_data_cache(code, date)

        stock["5d"] = stock["close"].rolling(window=5).mean()  # 周线
        stock["10d"] = stock["close"].rolling(window=10).mean()  # 半月线
        stock["20d"] = stock["close"].rolling(window=20).mean()  # 月线
        stock["60d"] = stock["close"].rolling(window=60).mean()  # 季度线
        stock["5-10d"] = (stock["5d"] - stock["10d"]) * 100 / stock["10d"]  # 周-半月线差
        stock["5-20d"] = (stock["5d"] - stock["20d"]) * 100 / stock["20d"]  # 周-月线差
        stock["return"] = np.log(stock["close"] / stock["close"].shift(1))

        mov_day = int(len(stock)/20)
        stock["mov_vol"] = stock["return"].rolling(window=mov_day).std() * math.sqrt(mov_day)
        return pd.Series(
            [
                date,
                code,
                numpy.get_last_valid_val(stock["10d"].to_list()),
                numpy.get_last_valid_val(stock["20d"].to_list()),
                numpy.get_last_valid_val(stock["5-10d"].to_list()),
                numpy.get_last_valid_val(stock["5-20d"].to_list()),
                numpy.get_last_valid_val(stock["5d"].to_list()),
                numpy.get_last_valid_val(stock["60d"].to_list()),
                numpy.get_last_valid_val(stock["mov_vol"].to_list()),
                numpy.get_last_valid_val(stock["return"].to_list())
            ],
            index=stock_name_list
        )


calculate_period_return_index = CalculatePeriodReturnIndex()

# main函数入口
if __name__ == '__main__':
    # 使用方法传递。
    tmp_datetime = executor.run_with_args(calculate_period_return_index.run)
