import datetime

from model.pro_trade_cal import pro_trade_cal
from svc.train.distribution import uniform_distribution
import pandas as pd


class TimeSeries:
    """
    根据参数对一个时间段的所有满足条件的股票进行抽样
    begin_date：开始时间点，日期类型，包含
    end_date：结束时间点，日期类型，包含
    total：样本总量，默认1万
    train_percent：各分量的比例，列表，累计总和为1（100%）,默认为[0.5, 0.5]
    distribution：时间序列上的分布函数（离散函数）
    """
    def __init__(
            self,
            begin_date,  # 时间序列开始时间
            end_date,  # 时间序列结束时间
            total=10000,  # 总数据量，默认为10000
            train_percent=0.8,  # 训练样本的总量，默认训练样本占总样本数的80%，其余的为测试样本
            distribution=uniform_distribution,  # 抽样分布函数，默认为均匀分布
    ):
        self.begin_date = begin_date
        self.end_date = end_date
        #  查询时间区间内所有的交易日数据
        trade_cal = pro_trade_cal.select_open_cal(self.begin_date, self.end_date)
        #  根据分布函数获取样本在区间内的分布情况，保存在total_cnt中
        trade_cal['total'] = distribution.calculate_distribute(
            x_max=trade_cal.index.values.max(),
            y_total=total
        )

        trade_cal.rename(columns={'cal_date': 'date'}, inplace=True)
        self.time_series = trade_cal[['date', 'total']].copy()
        self.time_series['train'] = (self.time_series['total'] * train_percent).round().astype('int')
        self.time_series['test'] = self.time_series['total'] - self.time_series['train']

    def __iter__(self):
        self.row_index = 0
        return self

    def __next__(self) -> pd.Series:
        if self.row_index >= self.time_series.shape[0]:
            raise StopIteration

        row = self.time_series.iloc[self.row_index]
        self.row_index += 1
        return row


if __name__ == '__main__':
    time_series = TimeSeries(
        begin_date=datetime.date(2018, 3, 20),
        end_date=datetime.date(2020, 3, 20),
        total=40000,
        train_percent=0.9,
        distribution=uniform_distribution,
    )
    print(time_series.time_series.head(10))
