#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import datetime
import logging
import os
import sys
import traceback

import pandas as pd
import tushare as ts


def run_with_args(run_fun):
    logging.info(f'Starting running func[{run_fun}] at {datetime.datetime.now()}')

    if len(sys.argv) == 3:
        tmp_year, tmp_month, tmp_day = sys.argv[1].split("-")
        loop = int(sys.argv[2])
        tmp_datetime = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day))
        for i in range(0, loop):
            tmp_datetime_new = tmp_datetime + datetime.timedelta(days=i)
            try:
                run_fun(tmp_datetime_new)
            except Exception as e:
                print("error :", e)
                traceback.print_exc()
    elif len(sys.argv) == 2:
        tmp_year, tmp_month, tmp_day = sys.argv[1].split("-")
        tmp_datetime = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day))
        try:
            run_fun(tmp_datetime)
        except Exception as e:
            print("error :", e)
            traceback.print_exc()
    else:
        try:
            run_fun(datetime.datetime.now())  # 使用当前时间
        except Exception as e:
            print("error :", e)
            traceback.print_exc()

    logging.info(f'Finish running func[{run_fun}] at {datetime.datetime.now()}')


# 设置基础目录，每次加载使用。
bash_stock_tmp = os.environ['HOME'] + "/data/cache/hist_data_cache/%s/%s/"
if not os.path.exists(bash_stock_tmp):
    os.makedirs(bash_stock_tmp)  # 创建多个文件夹结构。


# 增加读取股票缓存方法。加快处理速度。
def get_hist_data_cache(code, date_start, date_end):
    cache_dir = bash_stock_tmp % (date_end[0:7], date_end)
    # 如果没有文件夹创建一个。月文件夹和日文件夹。方便删除。
    # print("cache_dir:", cache_dir)
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    cache_file = cache_dir + "%s^%s.gzip.pickle" % (date_end, code)
    # 如果缓存存在就直接返回缓存数据。压缩方式。
    if os.path.isfile(cache_file):
        logging.info(f"Read cache file: {cache_file}")
        return pd.read_pickle(cache_file, compression="gzip")
    else:
        logging.info(f"Write cache file: {cache_file}")
        stock = ts.get_hist_data(code, start=date_start, end=date_end)
        if stock is None:
            return None
        stock = stock.sort_index(0)  # 将数据按照日期排序下。
        stock.to_pickle(cache_file, compression="gzip")
        return stock


def format_value(val):
    if val is None or type(val) is not str or val.find('.') == -1:
        return val

    try:
        return str(round(float(val), 4))
    except ValueError:
        return val

## TODO 需要想办法把import语句移到文件最上面

from libs.environment import Environment

environment = Environment()

from libs.mysql import MySql

mysql = MySql()


from libs.pandas import Pandas

pandas = Pandas()
