from lib.mysql import mysql
from lib.pandas import pandas


class TsTodayAll:

    def __init__(self):
        self.table_name = 'ts_today_all'

        self.select_sql = f"""
SELECT 
    `date`,
    `code`,
    `name`,
    `changepercent`,
    `trade`,
    `open`,
    `high`,
    `low`,
    `settlement`,
    `volume`,
    `turnoverratio`,
    `amount`,
    `per`,
    `pb`,
    `mktcap`,
    `nmc`
FROM
    stock_data.{self.table_name}"""

        self.where_clause = """
WHERE
    `date` = %s AND `trade` > 0
        AND `open` > 0
        AND trade <= 20
        AND `code` NOT LIKE '300%%'
        AND `name` NOT LIKE '%%st%%'"""

        self.pagination_filter = """
LIMIT %s , %s"""

    def count(self, date):
        return mysql.count_with_where_clause(
            table_name=self.table_name,
            params=[date.strftime("%Y%m%d")],
            clause=self.where_clause,
        )

    def select(self, date):
        return pandas.create_data_frame_by_sql(
            sql=self.select_sql + self.where_clause,
            params=[date.strftime("%Y%m%d")],
            subset='code',
            keep='last'
        )

    def paged_select(self, date, begin, size):
        return pandas.create_data_frame_by_sql(
            sql=self.select_sql + self.where_clause + self.pagination_filter,
            params=[date.strftime("%Y%m%d"), begin, size],
            subset='code',
            keep='last'
        )


ts_today_all = TsTodayAll()