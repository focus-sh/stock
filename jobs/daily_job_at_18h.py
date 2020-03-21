#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
import logging
import time

import tushare as ts

from libs.executor import executor
from libs.mysql import mysql
from libs.tushare import tushare


class DailyJobAt18H:

    @staticmethod
    def download_index(date):
        tushare.download_data(
            svc_name='get_index',
            primary_keys=["date", "code"],
            appendix={
                'date': date.strftime("%Y%m%d")
            }
        )

    @staticmethod
    def download_today_all(date):
        tushare.download_data(
            svc_name='get_today_all',
            primary_keys=["date", "code"],
            appendix={
                'date': date.strftime("%Y%m%d")
            }
        )


daily_job = DailyJobAt18H()

if __name__ == '__main__':
    executor.run_with_args(daily_job.download_index)
    time.sleep(5)  # 停止5秒
    executor.run_with_args(daily_job.download_today_all)
