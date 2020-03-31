import logging
import random

import pandas as pd

from lib.datetime import datetime
from model.pro_trade_cal import pro_trade_cal
from model.ts_pro_bar import ts_pro_bar


class Sample:
    def __init__(
            self,
            begin_date,  # 开始抽样时间
            end_date,  # 结束抽样时间
            sample_cnt,  # 样本总数
            ratio,  # 训练集占总样本的比例（样本数=训练数据+测试数据）
            distribution,  # 抽样分布函数
    ):
        self.begin_date = begin_date
        self.end_date = end_date
        #  查询时间区间内所有的交易日数据
        self.trade_cal = pro_trade_cal.select_open_cal(self.begin_date, self.end_date)
        #  根据分布函数获取样本在区间内的分布情况，保存在total_cnt中
        self.trade_cal['total_cnt'] = distribution.calculate_distribute(
            x_max=self.trade_cal.index.values.max(),
            y_total=sample_cnt
        )
        #  根据训练集比例，计算训练集样本数量
        self.trade_cal['training_cnt'] = (self.trade_cal['total_cnt'] * ratio).round().astype('int')
        #  计算测试集样本数量
        self.trade_cal['testing_cnt'] = (self.trade_cal['total_cnt'] - self.trade_cal['training_cnt']).astype('int')

    def do_service(self):
        all_pro_bar = ts_pro_bar.select_valid_record_between_date(self.begin_date, self.end_date)
        selected = self.trade_cal.apply(
            lambda row: self.select_stock(
                cal_date=row['cal_date'],
                training_cnt=row['training_cnt'],
                testing_cnt=row['testing_cnt']
            ),
            axis=1
        )
        print(selected.head(10))

    @staticmethod
    def select_stock(cal_date, training_cnt, testing_cnt):
        logging.info(f'Pick up stock point(cal_date={cal_date}, training_cnt={training_cnt}, testing_cnt={testing_cnt})')
        stock_name_list = ['date', 'training', 'testing']
        stocks = ts_pro_bar.select_valid_record_between_date(datetime.str_to_date(cal_date))
        total_selected = random.sample(range(0, stocks.shape[0]), training_cnt+testing_cnt)
        training = []
        testing = []
        for index, item in enumerate(total_selected):
            ts_code = stocks.iloc[index]['ts_code']
            if index < training_cnt:
                training.append(ts_code)
            else:
                testing.append(ts_code)

        return pd.Series([cal_date, training, testing], index=stock_name_list)
