import math

from lib.tushare import tushare
from model.pro_stock_basic import pro_stock_basic


class DownloadProBar:

    def __init__(self):
        self.batch_size = 100

    def run(self):
        # 分页取所有pro_stock_basic中的数据
        count = pro_stock_basic.count()
        end = int(math.ceil(float(count) / self.batch_size) * self.batch_size)
        for i in range(0, end, self.batch_size):
            data = pro_stock_basic.paged_select(i, self.batch_size)
            for _, row in data.iterrows():
                # 对每条数据执行：
                ts_code = row['ts_code']
                start_date = row['list_date']
                self.download(ts_code=ts_code, start_date=start_date)
                #  1. 查询ts_pro_bar表中最大的时间
                #  2. 取ts_pro_bar表中最大的时间和pro_stock_basic中的list_date的较大值
                # 调用pro_bar接口下载并保存数据

    @staticmethod
    def download(ts_code, start_date):
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


download_pro_bar = DownloadProBar()
