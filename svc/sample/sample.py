import pandas as pd
from model.pro_trade_cal import pro_trade_cal


class Sample:
    def __init__(
            self,
            begin_date,  # 开始抽样时间
            end_date,  # 结束抽样时间
            sample_cnt,  # 样本总数
            ratio,  # 训练集占总样本的比例（样本数=训练数据+测试数据）
            distribution,  # 抽样分布函数
    ):
        #  查询时间区间内所有的交易日数据
        self.trade_cal = pro_trade_cal.select_open_cal(begin_date, end_date)
        #  根据分布函数获取样本在区间内的分布情况，保存在total_cnt中
        self.trade_cal['total_cnt'] = distribution.calculate_distribute(
            x_max=self.trade_cal.index.values.max(),
            y_total=sample_cnt
        )
        #  根据训练集比例，计算训练集样本数量
        self.trade_cal['training_cnt'] = (self.trade_cal['total_cnt'] * ratio).round()
        #  计算测试集样本数量
        self.trade_cal['testing_cnt'] = self.trade_cal['total_cnt'] - self.trade_cal['training_cnt']

    def do_service(self):
        # 遍历 trade_cal，按天取出所有数据
        print(self.trade_cal.head(20))
        pass


class Distribution:
    def calculate_distribute(self, x_max, y_total):
        distribute = []
        remain = 0.0
        for x in range(0, x_max+1):
            probability = self.probability(x_max, x)
            cnt_float = y_total * probability + remain
            distribute.append(int(cnt_float))
            remain = cnt_float - distribute[x]

        return distribute


class UniformDistribution(Distribution):
    @staticmethod
    def probability(x_max, x):
        return 1 / (x_max + 1)


uniform_distribution = UniformDistribution()
