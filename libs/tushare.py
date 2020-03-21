import logging

from libs.mysql import mysql
import tushare as ts


class TuShare:
    def download_data(self, svc_name, primary_keys, params=None, appendix={}, table_name=None):
        logging.info(f'Downloading data from service<{svc_name}> with keys<{primary_keys}>')
        args = [] if params is None else params['args']
        kwargs = {} if params is None else params['kwargs']
        data = getattr(ts, svc_name)(*args, **kwargs)
        if data is None or len(data) == 0:
            logging.warning(f'No data found by calling service<{svc_name}>.')
            return

        # Add constant columns if has any
        for key in appendix:
            data[key] = appendix[key]

        # remove duplicated items when drop_duplicates is True
        if self.should_drop_duplicates(data.index.name, primary_keys):
            data = data.drop_duplicates(subset=primary_keys, keep="last")

        if table_name is None:
            table_name = tushare.get_table_name(svc_name)

        # 为了便于显示，浮点数均保留4位有效数字
        mysql.insert_db(data.round(4), table_name, primary_keys)
        logging.info(f'Finish downloading data from service[{svc_name}]')

    @staticmethod
    def should_drop_duplicates(index_name, primary_keys):
        if index_name is None:
            return True

        if isinstance(primary_keys, str):
            return primary_keys != index_name

        return index_name not in primary_keys

    @staticmethod
    def get_table_name(svc_name):
        table_name = svc_name
        if table_name.startswith('get_'):
            table_name = table_name.replace('get_', '')

        return 'ts_' + table_name


tushare = TuShare()
