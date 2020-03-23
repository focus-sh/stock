#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import heapq

import pandas as pd

from lib.executor import executor
from lib.mysql import mysql
from lib.numpy import numpy
from lib.pandas import pandas
from model.stock_statistics_buy_lite import stock_statistics_buy_lite
from model.stock_statistics_lite import stock_statistics_lite


class CalculateStockStatisticsBuy:

    def run(self, date):
        data = stock_statistics_lite.select(
            date=date,
            min_change_percent=0,
            min_pb=0
        )

        # 输入 date 用作历史数据查询。
        statistics = pd.DataFrame(
            data={
                "date": data["date"],
                "code": data["code"],
            },
            index=data.index.values
        )

        statistics = statistics.apply(
            lambda row: self.calculate_statistics(
                code=row['code'],
                date=row['date'],
            ),
            axis=1
        )
        statistics.drop('date', axis=1, inplace=True)
        statistics = pd.merge(data, statistics, on=['code'], how='left')

        statistics = statistics[statistics["trade"] > statistics["wave_base"]]
        statistics = statistics[statistics["trade"] < statistics["wave_crest"]]

        statistics["up_rate"] = (statistics["wave_mean"].sub(statistics["trade"])).div(statistics["wave_crest"]).mul(100)

        statistics["buy"] = 1
        statistics["sell"] = 0
        statistics["today_trade"] = data["trade"]
        statistics["income"] = 0

        statistics = statistics.rename(columns={'date': 'buy_date'})
        stock_statistics_buy_lite.insert(statistics)

    @staticmethod
    def calculate_statistics(date, code):
        stock_name_list = ['date', 'code', 'wave_mean', 'wave_crest', 'wave_base']
        stock_data_list = [date, code]

        stock = pandas.get_stock_hist_data_cache(code, date)
        price_list = pd.Series(stock["close"].values).to_list()
        stock_data_list.append(numpy.get_valid_mean(price_list))

        max_point = 3

        stock_data_list.append(numpy.get_valid_nlargest_mean(max_point, price_list))
        stock_data_list.append(numpy.get_valid_nsmallest_mean(max_point, price_list))

        return pd.Series(stock_data_list, index=stock_name_list)


calculate_stock_statistics_buy = CalculateStockStatisticsBuy()

if __name__ == '__main__':
    executor.run_with_args(calculate_stock_statistics_buy.run)
