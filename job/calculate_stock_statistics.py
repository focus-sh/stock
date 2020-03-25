import math

import pandas as pd

from lib.executor import executor
from lib.stockstats import stockstats
from model.ss_stock_statistics import ss_stock_statistics
from model.stock_statistics import stock_statistics
from model.ts_today_all import ts_today_all


class StockStatsIndexCalculator:

    def __init__(self):
        self.batch_size = 100

    def run(self, date):
        ss_stock_statistics.delete(date)

        count = ts_today_all.count(date)
        end = int(math.ceil(float(count) / self.batch_size) * self.batch_size)

        for i in range(0, end, self.batch_size):
            data = ts_today_all.paged_select(date, i, self.batch_size)
            data = self.append_stock_stats(data)
            ss_stock_statistics.insert(data)

    @staticmethod
    def append_stock_stats(data):
        statistics = data.apply(
            lambda row: stockstats.calculate_statistics(
                code=row['code'],
                date=row['date'],
            ),
            axis=1
        )
        statistics.drop('date', axis=1, inplace=True)
        return pd.merge(data, statistics, on=['code'], how='left')


class StockStatisticsFilter:

    @staticmethod
    def filter(date):
        stock_statistics.delete(date)
        data = ss_stock_statistics.paged_select(
            date=date,
            min_kdjk=80,
            min_kdjd=70,
            min_kdjj=90,
            min_rsi_6=50,
            min_cci=100
        )
        stock_statistics.insert(data)


stock_statistics_filter = StockStatisticsFilter()
stock_stats_index_calculator = StockStatsIndexCalculator()

if __name__ == '__main__':
    executor.run_with_args(stock_stats_index_calculator.run)
    executor.run_with_args(stock_statistics_filter.filter)
