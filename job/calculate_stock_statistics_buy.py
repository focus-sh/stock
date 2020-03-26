#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import pandas as pd

from lib.executor import executor
from lib.stockstats import stockstats
from model.stock_statistics import stock_statistics
from model.stock_statistics_buy import stock_statistics_buy


class CalculateStockStatisticsBuy:

    def run(self, date):
        data = stock_statistics.select(
            date=date,
            min_change_percent=2,
            min_pb=0
        )

        statistics = data.apply(
            lambda row: stockstats.calculate_wave(
                code=row['code'],
                date=row['date'],
                max_point=3,
            ),
            axis=1
        )
        statistics.drop('date', axis=1, inplace=True)
        statistics = pd.merge(data, statistics, on=['code'], how='left')

        statistics = statistics[statistics["trade"] > statistics["wave_base"]]
        statistics = statistics[statistics["trade"] < statistics["wave_crest"]]

        statistics["up_rate"] = stockstats.calculate_up_rate(
            wave_mean=statistics["wave_mean"],
            trade=statistics["trade"],
            wave_crest=statistics["wave_crest"]
        )

        statistics["buy"] = 1
        statistics["sell"] = 0
        statistics["today_trade"] = statistics["trade"]
        statistics["income"] = 0

        statistics = statistics.rename(columns={'date': 'buy_date'})
        stock_statistics_buy.insert(statistics)


calculate_stock_statistics_buy = CalculateStockStatisticsBuy()

if __name__ == '__main__':
    executor.run_with_args(calculate_stock_statistics_buy.run)
