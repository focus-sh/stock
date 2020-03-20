#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
import logging
import time

import tushare as ts

from libs.executor import executor
from libs.mysql import mysql

"""
交易数据

http://tushare.org/trading.html#id2

股市交易时间为每周一到周五上午时段9:30-11:30，下午时段13:00-15:00。 周六、周日上海证券交易所、深圳证券交易所公告的休市日不交易。

"""


def stat_index_all(date):
    data = ts.get_index()
    if data is not None and len(data) > 0:
        data["date"] = date.strftime("%Y%m%d")  # 修改时间成为int类型。
        data = data.drop_duplicates(subset="code", keep="last")
        data.head(n=1)
        mysql.insert_db(data, "ts_index_all", False, "`date`,`code`")
    else:
        logging.warning('No data found by calling ts.get_index() function.')


def stat_today_all(date):
    data = ts.get_today_all().round(2)
    if data is not None and len(data) > 0:
        data["date"] = date.strftime("%Y%m%d")
        data = data.drop_duplicates(subset="code", keep="last")
        data.head(n=1)
        mysql.insert_db(data, "ts_today_all", False, "`date`,`code`")
    else:
        logging.warning('No data found by calling ts.get_today_all() function.')


if __name__ == '__main__':
    executor.run_with_args(stat_index_all)
    time.sleep(5)  # 停止5秒
    executor.run_with_args(stat_today_all)
