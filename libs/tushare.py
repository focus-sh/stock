from libs.mysql import mysql
import tushare as ts


class TuShare:

    def download_data(self, svc_name, primary_keys, table_name=None):
        if table_name is None:
            table_name = self.get_table_name(svc_name)

        mysql.insert_db(getattr(ts, svc_name)(), table_name, primary_keys)

    @staticmethod
    def get_table_name(svc_name):
        table_name = svc_name
        if table_name.startswith('get_'):
            table_name = table_name.replace('get_', '')

        return 'ts_' + table_name

tushare = TuShare()
