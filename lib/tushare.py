import logging

from lib.mysql import mysql
import tushare as ts
from lib.environment import environment as env


class TuShare:
    def __init__(self):
        self.token = env.get_env_with_def("TUSHARE_TOKEN", "133a09e917c71347aabc97a2b0fc3a4837a2487b643537c5d29f75a6")
        logging.info(f'Initializing TuShare Pro Api with token: {self.token}')
        ts.set_token(token=self.token)
        self.pro = ts.pro_api(token=self.token)
        logging.info(f'TuShare Pro Api Initialized.')

    def download_data_pro(self, svc_name=None, params=None, appendix={}, table_name=None, primary_keys=[]):
        logging.info(f'Downloading data from service(Pro)<{svc_name}> with keys<{primary_keys}>')
        data = self.call_remote(api=self.pro, svc_name=svc_name, params=params)
        self.save(
            data=data,
            appendix=appendix,
            table_name=table_name or self.get_table_name('pro_', svc_name),
            primary_keys=primary_keys
        )
        logging.info(f'Finish downloading data from service(Pro)[{svc_name}]')

    def download_data(self, svc_name=None, params=None, appendix={}, table_name=None, primary_keys=[], indexes=[]):
        logging.info(f'Downloading data from service<{svc_name}> with params<{params}> and keys<{primary_keys}>')
        data = self.call_remote(svc_name=svc_name, params=params)
        self.save(
            data=data,
            appendix=appendix,
            table_name=table_name or self.get_table_name(svc_name=svc_name),
            primary_keys=primary_keys,
            indexes=indexes
        )
        logging.info(f'Finish downloading data from service[{svc_name}]')

    @staticmethod
    def call_remote(api=ts, svc_name=None, params={}):
        args = (params or {}).get('args') or []
        kwargs = (params or {}).get('kwargs') or {}
        return getattr(api, svc_name)(*args, **kwargs)

    def save(self, data=None, appendix={}, table_name=None, primary_keys=[], indexes=[]):
        if data is None or len(data) == 0:
            logging.warning(f'No data found for table<{table_name}>.')
            return

        # Add constant columns if has any
        for key in appendix:
            data[key] = appendix[key]

        mysql.insert(data, table_name, primary_keys, indexes)

    @staticmethod
    def get_table_name(prefix='ts_', svc_name=None):
        table_name = svc_name
        if table_name.startswith('get_'):
            table_name = table_name.replace('get_', '')

        return prefix + table_name


tushare = TuShare()
