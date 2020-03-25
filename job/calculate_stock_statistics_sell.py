#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import pandas as pd
import stockstats

from lib.executor import executor
from lib.numpy import numpy
from lib.pandas import pandas
from model.stock_statistics_buy import stock_statistics_buy
from model.stock_statistics_sell import stock_statistics_sell


class CalculateStockStatisticsSell:

    def run(self, date):
        data = stock_statistics_buy.select(date)
        data["date"] = date.strftime("%Y%m%d")

        statistics = data.apply(
            lambda row: self.calculate_statistics(
                code=row['code'],
                date=row['date'],
            ),
            axis=1
        )
        statistics.drop('date', axis=1, inplace=True)
        statistics = pd.merge(data, statistics, on=['code'], how='left')
        statistics["income"] = (statistics["today_trade"] - statistics["trade"]) * 100

        stock_statistics_sell.delete(date)
        stock_statistics_sell.insert(statistics)

    def calculate_statistics(self, code, date):
        stock_name_list = ['code', 'date', 'today_trade', "sell_cci", "sell_kdjj", "sell_rsi_6", "sell", "buy"]
        stock_data_list = [code, date]

        stock = pandas.get_stock_hist_data_cache(code, date)
        stock_data_list.append(numpy.get_valid_ele(stock.tail(1)['close'].to_list(), 0))

        stock_stats = stockstats.StockDataFrame.retype(stock)
        stock_data_list.append(numpy.get_valid_ele(stock_stats["cci"].tail(1).to_list(), 0))
        stock_data_list.append(numpy.get_valid_ele(stock_stats["kdjj"].tail(1).to_list(), 0))
        stock_data_list.append(numpy.get_valid_ele(stock_stats["rsi_6"].tail(1).to_list(), 0))

        sell_indicator = self.get_sell_indicator(stock_data_list[3], stock_data_list[4], stock_data_list[5])
        stock_data_list.append(sell_indicator)
        stock_data_list.append(1-sell_indicator)
        return pd.Series(stock_data_list, index=stock_name_list)

    @staticmethod
    def get_sell_indicator(kdjj, rsi_6, cci):
        return 1 if kdjj > 80 and rsi_6 > 55 or cci > 100 else 0


calculate_stock_statistics_sell = CalculateStockStatisticsSell()

if __name__ == '__main__':
    tmp_datetime = executor.run_with_args(calculate_stock_statistics_sell.run)
