#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import pandas as pd

from lib.executor import executor
from lib.stockstats import stockstats
from model.period_wave_index import period_wave_index
from model.ts_today_all import ts_today_all


class CalculatePeriodWaveIndex:

    @staticmethod
    def run(date):
        data = ts_today_all.select(date)

        statistics = data.apply(
            lambda row: stockstats.calculate_wave(
                code=row['code'],
                date=row['date'],
                max_point=5
            ),
            axis=1
        )
        statistics.drop('date', axis=1, inplace=True)

        statistics = pd.merge(data, statistics, on=['code'], how='left')

        statistics["up_rate"] = stockstats.calculate_up_rate(
            trade=statistics["trade"],
            wave_mean=statistics["wave_mean"],
            wave_crest=statistics["wave_crest"]
        )

        period_wave_index.delete(date)
        period_wave_index.insert(statistics)


calculate_period_wave_index = CalculatePeriodWaveIndex()

# main函数入口
if __name__ == '__main__':
    # 使用方法传递。
    tmp_datetime = executor.run_with_args(calculate_period_wave_index.run)
