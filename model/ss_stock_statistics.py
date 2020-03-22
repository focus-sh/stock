from lib.mysql import mysql


class SsStockStatistics:

    def __init__(self):
        self.table_name = 'ss_stock_statistics'

    def delete(self, date):
        mysql.del_by_date(self.table_name,  date)

    def insert(self, data):
        mysql.insert_db(
            data=data,
            table_name=self.table_name,
            primary_keys=["date", "code"]
        )


ss_stock_statistics = SsStockStatistics()