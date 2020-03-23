from lib.mysql import mysql
from lib.pandas import pandas


class StockStatisticsBuyLite:

    def __init__(self):
        self.table_name = 'stock_statistics_buy_lite'

    def insert(self, data):
        mysql.insert_db(
            data=data,
            table_name=self.table_name,
            primary_keys=["buy_date", "code"]
        )


stock_statistics_buy_lite = StockStatisticsBuyLite()
