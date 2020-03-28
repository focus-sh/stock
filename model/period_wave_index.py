from lib.mysql import mysql


class PeriodWaveIndex:

    def __init__(self):
        self.table_name = 'period_wave_index'

    def insert(self, data):
        mysql.insert(
            data=data,
            table_name=self.table_name,
            primary_keys=["date", "code"]
        )

    def delete(self, date):
        mysql.del_by_date(self.table_name,  date)


period_wave_index = PeriodWaveIndex()
