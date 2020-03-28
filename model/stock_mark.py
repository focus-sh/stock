from lib.mysql import mysql


class StockMark:
    def __init__(self):
        self.table_name = 'stock_mark'

    def insert(self, data):
        mysql.insert(
            data=data,
            table_name=self.table_name,
            primary_keys=["ts_code", "trade_date", "type"]
        )


stock_mark = StockMark()
