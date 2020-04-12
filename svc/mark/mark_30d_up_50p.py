from lib.executor import executor
from lib.iterator import Iterator
from model.pro_stock_basic import pro_stock_basic
from model.stock_mark import stock_mark
from model.ts_pro_bar import ts_pro_bar


class Mark30dUp50p:
    def __init__(self):
        self.iterator = Iterator(pro_stock_basic)

    def run(self, **kwargs):
        stock_basic = pro_stock_basic.select()
        for row in self.iterator:
            ts_code = row['ts_code']
            stock_info = stock_basic.loc[stock_basic['ts_code'] == ts_code]
            trade_data = ts_pro_bar.select_all_valid_record(ts_code)
            self.mark_with_default_logic(stock_info, trade_data)
            self.mark_exclude_st_stock(stock_info, trade_data)

    @staticmethod
    def mark_with_default_logic(stock_info, trade_data):
        # 获取该股票历史股价记录数据（按时间顺序顺序排序，剔除Null值数据）
        # 按日期从小到大遍历所有数据
        # 获取当前日股票的收盘价
        # 获取未来30天内股票的最高收盘价
        # 若最高收盘价 >= 当前日收盘价 * 1.5 则标记为1，否则标记为0
        # 计算20天内的最高股价，窗口是从当前位置往前数共20条数据
        trade_data['d20_max'] = trade_data['close'].rolling(window=20).max()
        # 将数据往前移动20位，则当前行保存的d20_max的值为从今天之后的下个交易日T(包含)，到T+19（包含）之间的最高股价
        trade_data['d20_max'] = trade_data['d20_max'].shift(-20)
        # 计算data['d20_max']（未来二十个交易日中最高收盘价）是否高于data['close']（今天的收盘价）的1.5倍价格
        trade_data['d20_max'] = trade_data['d20_max'].fillna(-1.0)
        trade_data['mark'] = trade_data['d20_max'] > trade_data['close'] * 1.5
        trade_data['type'] = 'd20_max_default'

        stock_mark.insert(trade_data[['ts_code', 'trade_date', 'type', 'mark']])

    @staticmethod
    def mark_exclude_st_stock(stock_info, trade_data):
        stock_name: str = stock_info['name'].values[0]

        # 获取该股票历史股价记录数据（按时间顺序顺序排序，剔除Null值数据）
        # 按日期从小到大遍历所有数据
        # 获取当前日股票的收盘价
        # 获取未来30天内股票的最高收盘价
        # 标记（名称 d20_max_exclude_st)：
        #   1. 若股票名称中包含ST，则标记为-1（无效数据）
        #   1. 若最高收盘价 >= 当前日收盘价 * 1.5 则标记为1（正例）
        #   2. 其余数据标记为0（反例）
        trade_data['d20_max'] = trade_data['close'].rolling(window=20).max()
        trade_data['d20_max'] = trade_data['d20_max'].shift(-20)
        trade_data['d20_max'] = trade_data['d20_max'].fillna(-1.0)

        trade_data['type'] = 'd20_max_exclude_st'
        if stock_name.find('ST') != -1:
            trade_data['mark'] = -1
        else:
            trade_data['mark'] = trade_data['d20_max'] > trade_data['close'] * 1.5

        stock_mark.insert(trade_data[['ts_code', 'trade_date', 'type', 'mark']])


mark_30d_up_50p = Mark30dUp50p()

if __name__ == '__main__':
    executor.run_with_args(mark_30d_up_50p.run)
