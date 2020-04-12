from lib.mysql import mysql
from lib.pandas import pandas
import pandas as pd

class Model:

    def __init__(self):
        self.table_name = ''
        self.primary_keys = []
        self.indexes = []
        self.select_sql = ''

    def insert(self, data):
        mysql.insert(
            data=data,
            table_name=self.table_name,
            primary_keys=self.primary_keys,
            indexes=self.indexes
        )

    def select(self, sql, params):
        return pandas.create_data_frame_by_sql(
            sql=sql,
            params=params,
            subset=self.primary_keys,
            keep='last'
        )

