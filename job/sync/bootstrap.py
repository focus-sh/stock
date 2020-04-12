from job.job import Job
from job.sync.pro_bar_slice_cache import pro_bar_slice_cache
from lib.datetime import datetime
from lib.executor import executor
from lib.file.tensorflow_slice import file_system
from lib.iterator import Iterator
from lib.tushare import tushare
from model.pro_stock_basic import pro_stock_basic
from model.pro_trade_cal import pro_trade_cal
from model.ts_pro_bar import ts_pro_bar


class Bootstrap(Job):
    """
    在系统启动时运行，初始化系统状态以及同步相关数据
    """

    def run(self, **kwargs):
        self.sync(**kwargs)
        # 同步历史交易数据
        self.sync_his_data(**kwargs)
        self.sync_his_daily_basic(**kwargs)
        # 刷新本地缓存的日切股票清单
        pro_bar_slice_cache.refresh_cache()

    def sync_his_data(self, **kwargs):
        for row in Iterator(pro_stock_basic):
            self.sync_ts_pro_bar(
                ts_code=row['ts_code'],
                list_date=row['list_date'],
                today=kwargs['date']
            )

    def sync_his_daily_basic(self, **kwargs):
        for row in Iterator(pro_trade_cal):
            tushare.download_data(
                api=tushare.pro,
                svc_name='daily_basic',
                params={
                    'kwargs': {
                        'trade_date': row['cal_date'],
                        'ts_code': ''
                    }
                },
                primary_keys=["ts_code", "trade_date"],
                indexes=['ts_code', 'trade_date']
            )

    def sync_ts_pro_bar(self, ts_code, list_date, today):
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
    def cache_daily_open_stock(date):
        data = ts_pro_bar.select_valid_record_by_date(date)
        file_system.write(data)

    def init_sync_ts_svc(self, **kwargs) -> dict:
        return [
            {
                "svc_name": 'get_deposit_rate',
                "primary_keys": ["date", "deposit_type"]
            }, {
                "svc_name": 'get_loan_rate',
                "primary_keys": ["date", "loan_type"]
            }, {
                "svc_name": 'get_rrr',
                "primary_keys": "date"
            }, {
                "svc_name": 'get_money_supply',
                "primary_keys": "month"
            }, {
                "svc_name": 'get_money_supply_bal',
                "primary_keys": "year"
            }, {
                "svc_name": 'get_gdp_year',
                "primary_keys": "year"
            }, {
                "svc_name": 'get_gdp_quarter',
                "primary_keys": "quarter"
            }, {
                "svc_name": 'get_gdp_for',
                "primary_keys": "year"
            }, {
                "svc_name": 'get_gdp_pull',
                "primary_keys": "year"
            }, {
                "svc_name": 'get_gdp_contrib',
                "primary_keys": "year"
            }, {
                "svc_name": 'get_cpi',
                "primary_keys": "month"
            }, {
                "svc_name": 'get_ppi',
                "primary_keys": "month"
            }, {
                "svc_name": 'get_stock_basics',
                "primary_keys": "code"
            }, {
                "api": tushare.pro,
                "svc_name": 'stock_basic',
                "params": {
                    'kwargs': {'exchange': '', 'list_status': 'L'}
                },
                "primary_keys": "ts_code"
            }, {
                "api": tushare.pro,
                "svc_name": 'trade_cal',
                "params": {
                    'kwargs': {'exchange': ''}
                },
                "primary_keys": ['exchange', "cal_date"]
            },
        ]

    @staticmethod
    def calculate_start_date(stock, list_date):
        if stock is None or stock.empty or 'trade_date' not in stock:
            return list_date

        trade_date = stock.loc[0, 'trade_date']
        if list_date is None:
            return datetime.next_day(trade_date)

        return datetime.max(datetime.next_day(trade_date), list_date)


bootstrap = Bootstrap()

if __name__ == '__main__':
    executor.run_with_args(bootstrap.run)
