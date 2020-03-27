import datetime
import logging
import os
import shutil

import pandas as pd
from lib.mysql import mysql
from lib.environment import environment as env
import tushare as ts


class Pandas:

    def __init__(self):
        self.bash_stock_tmp = env.home() + "/data/cache/hist_data_cache/%s/%s/"

    def del_hist_data_cache(self, date):
        cache_dir = self.bash_stock_tmp % (date.strftime("%Y-%m-%d")[0:7], (date.strftime("%Y-%m-%d")))
        if os.path.exists(cache_dir):
            shutil.rmtree(cache_dir)
            logging.info(f"remove cache dir force :{cache_dir}")

    def get_stock_hist_data_cache(self, code, date):
        date_end = datetime.datetime.strptime(date, "%Y%m%d")
        date_start = (date_end + datetime.timedelta(days=-300)).strftime("%Y-%m-%d")
        date_end = date_end.strftime("%Y-%m-%d")

        stock = self.get_hist_data_cache(code, date_start, date_end)
        return stock.sort_index(axis=0)  # 按日期排序

    def get_hist_data_cache(self, code, date_start, date_end):
        cache_dir = self.bash_stock_tmp % (date_end[0:7], date_end)

        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        cache_file = cache_dir + "%s^%s.gzip.pickle" % (date_end, code)
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

    @staticmethod
    def create_data_frame_by_sql(sql, params, subset, keep):
        logging.info(f"Create DataFrame with sql<{sql}\r\n> and params<{params}>")
        data = pd.read_sql(sql=sql, con=mysql.engine(), params=params)
        return data.drop_duplicates(subset, keep)


pandas = Pandas()
