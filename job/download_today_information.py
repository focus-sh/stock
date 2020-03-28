#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
import time

from lib.executor import executor
from lib.pandas import pandas
from lib.tushare import tushare


class DelHisCacheData:
    @staticmethod
    def run(date):
        pandas.del_hist_data_cache(date)


class DownloadTopList:
    @staticmethod
    def run(date):
        tushare.download_data(
            svc_name='top_list',
            params={
                'args': [date.strftime("%Y-%m-%d")],
                'kwargs': {'retry_count': 3}
            },
            appendix={
                'date': date.strftime("%Y%m%d")
            },
            primary_keys=["date", "code"]
        )


class DownloadIndex:
    @staticmethod
    def run(date):
        tushare.download_data(
            svc_name='get_index',
            appendix={
                'date': date.strftime("%Y%m%d")
            },
            primary_keys=["date", "code"]
        )


class DownloadTodayAll:
    @staticmethod
    def run(date):
        tushare.download_data(
            svc_name='get_today_all',
            appendix={
                'date': date.strftime("%Y%m%d")
            },
            primary_keys=["date", "code"]
        )


del_his_cache_data = DelHisCacheData()
download_top_list = DownloadTopList()
download_index = DownloadIndex()
download_today_all = DownloadTodayAll()


if __name__ == '__main__':
    executor.run_with_args(del_his_cache_data.run)
    executor.run_with_args(download_top_list.run)
    executor.run_with_args(download_index.run)
    time.sleep(5)  # 停止5秒
    executor.run_with_args(download_today_all.run)

