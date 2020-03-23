from lib.mysql import mysql


class StockStatisticsLite:

    def __init__(self):
        self.table_name = 'stock_statistics_lite'

    def delete(self, date):
        mysql.del_by_date(self.table_name,  date)

    def insert(self, data):
        mysql.insert_db(
            data=data,
            table_name=self.table_name,
            primary_keys=["date", "code"]
        )


stock_statistics_lite = StockStatisticsLite()
