from lib.mysql import mysql
from lib.pandas import pandas


class ProStockBasic:

    def __init__(self):
        self.table_name = 'pro_stock_basic'

        self.select_sql = f"""
SELECT 
    a.ts_code,
    a.symbol,
    a.name,
    a.area,
    a.industry,
    a.market,
    a.list_date
FROM
    stock_data.{self.table_name} a"""

        self.where_clause = """
WHERE
    1 = 1"""

        self.pagination_filter = """
LIMIT %s , %s"""

    def count(self):
        return mysql.count_with_where_clause(
            table_name=self.table_name,
            params=[],
            clause=self.where_clause,
        )

    def select(self):
        return pandas.create_data_frame_by_sql(
            sql=self.select_sql + self.where_clause,
            params=[],
            subset='ts_code',
            keep='last'
        )

    def paged_select(self, begin, size):
        return pandas.create_data_frame_by_sql(
            sql=self.select_sql + self.where_clause + self.pagination_filter,
            params=[begin, size],
            subset='ts_code',
            keep='last'
        )


pro_stock_basic = ProStockBasic()
