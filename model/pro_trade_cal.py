from lib.mysql import mysql
from lib.pandas import pandas


class ProTradeCal:

    def __init__(self):
        self.table_name = 'pro_trade_cal'
        self.primary_keys = ['exchange', 'cal_date']

        self.select_sql = f"""
SELECT 
    exchange, cal_date, is_open
FROM
    stock_data.{self.table_name} """

        self.select_open_cal_where_clause = f'''
WHERE
    is_open = 1 AND cal_date >= %s AND cal_date <= %s
ORDER BY cal_date'''

        self.where_clause = '''
WHERE
    is_open = 1'''

        self.pagination_filter = """
LIMIT %s, %s"""

    def select_open_cal(self, begin, end):
        return pandas.create_data_frame_by_sql(
            sql=self.select_sql + self.select_open_cal_where_clause,
            params=[begin.strftime("%Y%m%d"), end.strftime("%Y%m%d")],
            subset=self.primary_keys,
            keep='last'
        )

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
            subset=self.primary_keys,
            keep='last'
        )

    def paged_select(self, begin, size):
        return pandas.create_data_frame_by_sql(
            sql=self.select_sql + self.where_clause + self.pagination_filter,
            params=[begin, size],
            subset=self.primary_keys,
            keep='last'
        )


pro_trade_cal = ProTradeCal()
