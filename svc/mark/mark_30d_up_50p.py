from lib.executor import executor
from lib.iterator import Iterator
from model.pro_stock_basic import pro_stock_basic
from model.stock_mark import stock_mark
from model.ts_pro_bar import ts_pro_bar


class Mark30dUp50p:
    def __init__(self):
        self.iterator = Iterator(pro_stock_basic)

    def run(self, **kwargs):
        for row in self.iterator:
            self.mark(row['ts_code'])

    def mark(self, ts_code):
        # 获取该股票历史股价记录数据（按时间顺序顺序排序，剔除Null值数据）
        # 按日期从小到大遍历所有数据
        # 获取当前日股票的收盘价
        # 获取未来30天内股票的最高收盘价
        # 若最高收盘价 >= 当前日收盘价 * 1.5 则标记为1，否则标记为0
        data = ts_pro_bar.select_all_valid_record(ts_code)

        # 计算20天内的最高股价，窗口是从当前位置往前数共20条数据
        data['d20_max'] = data['close'].rolling(window=20).max()
        # 将数据往前移动20位，则当前行保存的d20_max的值为从今天之后的下个交易日T(包含)，到T+19（包含）之间的最高股价
        data['d20_max'] = data['d20_max'].shift(-20)
        # 计算data['d20_max']（未来二十个交易日中最高收盘价）是否高于data['close']（今天的收盘价）的1.5倍价格
        data['d20_max'] = data['d20_max'].fillna(-1.0)
        data['mark'] = data['d20_max'] > data['close'] * 1.5
        data['type'] = 'd20_max'

        stock_mark.insert(data[['ts_code', 'trade_date', 'type', 'mark']])


mark_30d_up_50p = Mark30dUp50p()

if __name__ == '__main__':
    executor.run_with_args(mark_30d_up_50p.run)
