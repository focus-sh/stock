import logging
import math

from model.pro_stock_basic import pro_stock_basic


class Mark30dUp50p:
    """对股票历史数据打标，如果从当天向后30天内，股票涨幅超过50%的，标记为1，否则为0
    """
    def run(self):
        # 分页选择股票清单
        # 对每一支股票
            # 获取该股票历史股价记录数据（按时间顺序顺序排序，剔除Null值数据）
            # 按日期从小到大遍历所有数据
            # 获取当前日股票的收盘价
            # 获取未来30天内股票的最高收盘价
            # 若最高收盘价 >= 当前日收盘价 * 1.5 则标记为1，否则标记为0
        logging.info(f'Downloading stock history data using pro_bar svc')
        count = pro_stock_basic.count()
        end = int(math.ceil(float(count) / self.batch_size) * self.batch_size)
        for i in range(0, end, self.batch_size):
            data = pro_stock_basic.paged_select(i, self.batch_size)
            for _, row in data.iterrows():
                pass
        pass


mark_30d_up_50p = Mark30dUp50p()
