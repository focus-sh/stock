from libs.mysql import mysql
import tushare as ts


class TuShare:

    @staticmethod
    def download_data(svc_name, table_name, primary_keys, write_index=False):
        mysql.insert_db(getattr(ts, svc_name)(), table_name, primary_keys, write_index)


tushare = TuShare()
