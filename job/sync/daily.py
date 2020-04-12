from datetime import datetime

from lib.executor import executor
from job.job import Job
from lib.tushare import tushare


class DailySync(Job):
    def run(self, **kwargs):
        self.sync(**kwargs)

    def init_sync_ts_svc(self, **kwargs) -> dict:
        date = kwargs['date'] or datetime.date()
        # 需要初始化同步的所有数据服务清单
        return [
            {
                "svc_name": 'top_list',
                "params": {
                    'args': [date.strftime("%Y-%m-%d")],
                    'kwargs': {'retry_count': 3}
                },
                "appendix": {
                    'date': date.strftime("%Y%m%d")
                },
                "primary_keys": ["date", "code"]
            }, {
                "svc_name": 'get_index',
                "appendix": {
                    'date': date.strftime("%Y%m%d")
                },
                "primary_keys": ["date", "code"]
            }, {
                "svc_name": 'get_today_all',
                "appendix": {
                    'date': date.strftime("%Y%m%d")
                },
                "primary_keys": ["date", "code"]
            }, {
                "api": tushare.pro,
                "svc_name": 'daily_basic',
                "params": {
                    'kwargs': {
                        'trade_date': date.strftime("%Y%m%d"),
                        'ts_code': ''
                    }
                },
                "primary_keys": ["ts_code", "trade_date"]
            }
        ]


daily_sync = DailySync()

if __name__ == '__main__':
    executor.run_with_args(daily_sync.run)

