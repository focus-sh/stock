from lib.datetime import datetime
from lib.executor import executor
from lib.tushare import tushare
from model.ts_pro_bar import ts_pro_bar
from svc.pro_stock_basic_iterator import ProStockBasicIterator


class DownloadProBar(ProStockBasicIterator):

    def __init__(self):
        self.batch_size = 100

    def do_service(self, date, row):
        self.download(ts_code=row['ts_code'], list_date=row['list_date'], today=date)

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
            primary_keys=["ts_code", "trade_date"],
            indexes=['trade_date']
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
