#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from libs.executor import executor
from libs.pandas import pandas
from libs.tushare import tushare


class DailyJob:

    def run(self, date):
        pandas.del_hist_data_cache(date)

        tushare.download_data(
            svc_name='top_list',
            params={
                'args': [date.strftime("%Y-%m-%d")],
                'kwargs': {'retry_count': 6}
            },
            primary_keys=["date", "code"],
            appendix={
                'date': date.strftime("%Y%m%d")
            }
        )


daily_job = DailyJob()

if __name__ == '__main__':
    executor.run_with_args(daily_job.run)
