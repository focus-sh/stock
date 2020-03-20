import logging
import os

import pandas as pd
from libs.mysql import mysql
from libs.environment import environment as env
import tushare as ts


class Pandas:

    def __init__(self):
        self.bash_stock_tmp = env.home() + "/data/cache/hist_data_cache/%s/%s/"

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
        logging.info(f"Create DataFrame with sql[{sql}] and params[{params}]")
        data = pd.read_sql(sql=sql, con=mysql.engine(), params=params)
        logging.info(f"Drop duplicate records in DataFrame by subset=[{subset}] and keep[{keep}]")
        return data.drop_duplicates(subset, keep)


pandas = Pandas()
