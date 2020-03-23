#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import heapq

import pandas as pd

from lib.executor import executor
from lib.mysql import mysql
from lib.numpy import numpy
from lib.pandas import pandas
from model.stock_statistics_lite import stock_statistics_lite


class GuessIndicatorsLiteBuyDailyJob:

    def stat_all_lite(self, date):
        # 要操作的数据库表名称。
        table_name = "stock_statistics_lite"

        data = stock_statistics_lite.select(
            date=date,
            min_change_percent=0,
            min_pb=0
        )

        # 输入 date 用作历史数据查询。
        stock_merge = pd.DataFrame(
            data={
                "date": data["date"],
                "code": data["code"],
                "wave_mean": data["trade"],
                "wave_crest": data["trade"],
                "wave_base": data["trade"]
            },
            index=data.index.values
        )

        stock_merge = stock_merge.apply(
            lambda row: self.calculate_statistics(
                code=row['code'],
                date=row['date'],
            ),
            axis=1
        )
        del stock_merge["date"]
        data = pd.merge(data, stock_merge, on=['code'], how='left')

        data = data[data["trade"] > data["wave_base"]]
        data = data[data["trade"] < data["wave_crest"]]

        data["up_rate"] = (data["wave_mean"].sub(data["trade"])).div(data["wave_crest"]).mul(100)

        data["buy"] = 1
        data["sell"] = 0
        data["today_trade"] = data["trade"]
        data["income"] = 0

        data = data.rename(columns={'date': 'buy_date'})

        try:
            mysql.insert_db(
                data=data,
                table_name=table_name,
                primary_keys=["buy_date", "code"]
            )
            print("insert_db")
        except Exception as e:
            print("error :", e)

    def calculate_statistics(self, date, code):
        stock_name_list = ['date', 'code', 'wave_mean', 'wave_crest', 'wave_base']
        stock_data_list = [date, code]

        stock = pandas.get_stock_hist_data_cache(code, date)
        price_list = pd.Series(stock["close"].values).to_list()
        stock_data_list.append(numpy.get_valid_mean(price_list))

        max_point = 3

        stock_data_list.append(numpy.get_valid_nlargest_mean(max_point, price_list))
        stock_data_list.append(numpy.get_valid_nsmallest_mean(max_point, price_list))

        return pd.Series(stock_data_list, index=stock_name_list)


daily_job = GuessIndicatorsLiteBuyDailyJob()

if __name__ == '__main__':
    executor.run_with_args(daily_job.stat_all_lite)
