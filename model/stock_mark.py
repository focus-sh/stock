from lib.mysql import mysql
from model.model import Model


class StockMark(Model):
    def __init__(self):
        self.table_name = 'stock_mark'
        self.primary_keys = ["ts_code", "trade_date", "type"]
        self.indexes = ['trade_date', 'type']

        self.select_sql = f'''
SELECT 
    ts_code, trade_date, type, mark
FROM
    {self.table_name}'''

        self.select_by_data_and_type_and_mark_where_clause = f'''
WHERE
    trade_date = %s AND type = %s
        AND mark = %s'''

    def insert(self, data):
        mysql.insert(
            data=data,
            table_name=self.table_name,
            primary_keys=self.primary_keys,
            indexes=self.indexes
        )

    def select_by_data_and_type_and_mark(self, date, type, mark):
        sql = self.select_sql + self.select_by_data_and_type_and_mark_where_clause
        return self.select(sql, [date.strftime("%Y%m%d"), type, mark])


stock_mark = StockMark()
