#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
import logging
import os
import shutil

import tushare as ts

from libs.executor import executor
from libs.mysql import mysql


def stat_all(date):
    cache_dir = common.bash_stock_tmp % (date.strftime("%Y-%m-%d")[0:7], (date.strftime("%Y-%m-%d")))
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)
        logging.info(f"remove cache dir force :{cache_dir}")

    data = ts.top_list(date.strftime("%Y-%m-%d"))

    if data is not None and len(data) > 0:
        data["date"] = date.strftime("%Y%m%d")
        data = data.drop_duplicates(subset="code", keep="last")
        data.head(n=1)
        mysql.insert_db(data, "ts_top_list", False, "`date`,`code`")
    else:
        logging.warning(f'No data found by calling ts.top_list({date.strftime("%Y-%m-%d")}) function.')


if __name__ == '__main__':
    executor.run_with_args(stat_all)
