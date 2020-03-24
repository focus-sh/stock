from lib.mysql import mysql
from lib.pandas import pandas


class StockStatisticsBuy:

    def __init__(self):
        self.table_name = 'stock_statistics_buy'

        self.select_sql = f"""
SELECT 
    `buy_date`,
    `code`,
    `name`,
    `changepercent`,
    `trade`,
    `turnoverratio`,
    `pb`,
    `kdjj`,
    `rsi_6`,
    `cci`,
    `wave_base`,
    `wave_crest`,
    `wave_mean`,
    `up_rate`
FROM
    stock_data.{self.table_name}"""

        self.where_clause = """
WHERE
    `buy_date` <= %s"""

    def insert(self, data):
        mysql.insert_db(
            data=data,
            table_name=self.table_name,
            primary_keys=["buy_date", "code"]
        )

    def select(self, max_date):
        return pandas.create_data_frame_by_sql(
            sql=self.select_sql + self.where_clause,
            params=[max_date.strftime("%Y%m%d")],
            subset='code',
            keep='last'
        )


stock_statistics_buy = StockStatisticsBuy()
