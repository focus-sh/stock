import math

import pandas as pd

from lib.executor import executor
from lib.mysql import mysql
from lib.stockstats import stockstats
from model.ss_stock_statistics import ss_stock_statistics
from model.ts_today_all import ts_today_all


class GuessIndicatorsDailyJob:

    def stat_all_lite(self, date):
        sql_1 = """
                SELECT `date`, `code`, `name`, `changepercent`, `trade`, `open`, `high`, `low`, 
                                `settlement`, `volume`, `turnoverratio`, `amount`, `per`, `pb`, `mktcap`,
                                 `nmc` ,`kdjj`,`rsi_6`,`cci`
                            FROM stock_data.guess_indicators_daily WHERE `date` = %s 
                            and kdjk >= 80 and kdjd >= 70 and kdjj >= 90  and rsi_6 >= 50  and cci >= 100
        """

        try:
            del_sql = " DELETE FROM `stock_data`.`guess_indicators_lite_daily` WHERE `date`= '%s' " % date.strftime(
                "%Y%m%d")
            mysql.insert(del_sql)
        except Exception as e:
            print("error :", e)

        try:
            data = pd.read_sql(sql=sql_1, con=mysql.engine(), params=[date.strftime("%Y%m%d")])
            data = data.drop_duplicates(subset="code", keep="last")
        except Exception as e:
            print('error :', e)

        try:
            mysql.insert_db(data, "guess_indicators_lite_daily", "`date`,`code`", False)
        except Exception as e:
            print("error :", e)


class StockStatsIndexCalculator:

    def __init__(self):
        self.batch_size = 100
        self.table_name = 'stock_stats_index'

    def run(self, date):
        ss_stock_statistics.delete(date)

        count = ts_today_all.count(date)
        end = int(math.ceil(float(count) / self.batch_size) * self.batch_size)

        for i in range(0, end, self.batch_size):
            data = ts_today_all.select(date, i, self.batch_size)
            data = self.append_stock_stats(data)
            ss_stock_statistics.insert(data)

    @staticmethod
    def append_stock_stats(data):
        statistics = pd.DataFrame(
            data={
                'date': data['date'],
                'code': data['code'],
            },
            index=data.index.values)
        statistics = statistics.apply(
            lambda row: stockstats.calculate_statistics(
                code=row['code'],
                date=row['date'],
            ),
            axis=1
        )
        statistics.drop('date', axis=1, inplace=True)
        return pd.merge(data, statistics, on=['code'], how='left')


daily_job = GuessIndicatorsDailyJob()
stock_stats_index_calculator = StockStatsIndexCalculator()

if __name__ == '__main__':
    executor.run_with_args(stock_stats_index_calculator.run)
    executor.run_with_args(daily_job.stat_all_lite)
