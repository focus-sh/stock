import logging
import math

from lib.datetime import datetime
from lib.executor import executor
from lib.tushare import tushare
from model.pro_stock_basic import pro_stock_basic
from model.ts_pro_bar import ts_pro_bar


class DownloadProBar:

    def __init__(self):
        self.batch_size = 100

    def run(self, date):
        logging.info(f'Downloading stock history data using pro_bar svc')
        count = pro_stock_basic.count()
        end = int(math.ceil(float(count) / self.batch_size) * self.batch_size)
        for i in range(0, end, self.batch_size):
            data = pro_stock_basic.paged_select(i, self.batch_size)
            for _, row in data.iterrows():
                self.download(ts_code=row['ts_code'], list_date=row['list_date'], today=date)
        logging.info(f'Finish downloading stock history data using pro_bar svc')

    def download(self, ts_code, list_date, today):
        stock = ts_pro_bar.select_latest_record(ts_code)
        start_date = self.calculate_start_date(stock, list_date)

        # 判断start_date时间是不是大于当前时间
        if not start_date or datetime.str_to_date(datetime.next_day(start_date)) > today:
            return

        tushare.download_data(
            svc_name='pro_bar',
            params={
                'kwargs': {
                    'ts_code': ts_code,
                    'adj': 'qfq',
                    'start_date': start_date
                }
            },
            primary_keys=["ts_code", "trade_date"]
        )

    @staticmethod
    def calculate_start_date(stock, list_date):
        if stock is None or stock.empty or 'trade_date' not in stock:
            return list_date

        trade_date = stock.loc[0, 'trade_date']
        if list_date is None:
            return datetime.next_day(trade_date)

        return datetime.max(datetime.next_day(trade_date), list_date)


download_pro_bar = DownloadProBar()

if __name__ == '__main__':
    executor.run_with_args(download_pro_bar.run)
