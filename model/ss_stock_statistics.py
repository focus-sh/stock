from lib.mysql import mysql
from lib.pandas import pandas


class SsStockStatistics:

    def __init__(self):
        self.table_name = 'ss_stock_statistics'

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
    `nmc`,
    `kdjj`,
    `rsi_6`,
    `cci`
FROM
    stock_data.{self.table_name}"""

        self.where_clause = """
WHERE
    `date` = %s AND kdjk >= %s
        AND kdjd >= %s
        AND kdjj >= %s
        AND rsi_6 >= %s
        AND cci >= %s"""

    def insert(self, data):
        mysql.insert_db(
            data=data,
            table_name=self.table_name,
            primary_keys=["date", "code"]
        )

    def delete(self, date):
        mysql.del_by_date(self.table_name,  date)

    def select(self, date, min_kdjk, min_kdjd, min_kdjj, min_rsi_6, min_cci):
        return pandas.create_data_frame_by_sql(
            sql=self.select_sql + self.where_clause,
            params=[date.strftime("%Y%m%d"), min_kdjk, min_kdjd, min_kdjj, min_rsi_6, min_cci],
            subset='code',
            keep='last'
        )


ss_stock_statistics = SsStockStatistics()
