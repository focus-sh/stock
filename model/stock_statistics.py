from lib.mysql import mysql
from lib.pandas import pandas


class StockStatistics:

    def __init__(self):
        self.table_name = 'stock_statistics'

        self.select_sql = f"""
SELECT 
    `date`,
    `code`,
    `name`,
    `changepercent`,
    `trade`,
    `turnoverratio`,
    `pb`,
    `kdjj`,
    `rsi_6`,
    `cci`
FROM
    stock_data.{self.table_name}"""

        self.where_clause = """
WHERE
    `date` = %s
        AND `changepercent` > %s
        AND `pb` > %s"""

    def insert(self, data):
        mysql.insert_db(
            data=data,
            table_name=self.table_name,
            primary_keys=["date", "code"]
        )

    def delete(self, date):
        mysql.del_by_date(self.table_name,  date)

    def select(self, date, min_change_percent, min_pb):
        return pandas.create_data_frame_by_sql(
            sql=self.select_sql + self.where_clause,
            params=[date.strftime("%Y%m%d"), min_change_percent, min_pb],
            subset='code',
            keep='last'
        )


stock_statistics = StockStatistics()
