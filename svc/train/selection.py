import random

from pandas import DataFrame

from lib.datetime import datetime
from lib.file.tensorflow_slice import TensorflowSliceFileSystem
from model.ts_pro_bar import ts_pro_bar
from svc.train.distribution import uniform_distribution
from svc.train.time_series import TimeSeries


class Selection:
    """
    取样器，根据时序分布数据、缓存的每日股票清单数据生成日切的样本清单
    model_name：模型名称，用于确定生成的日切文件的存储位置，后续计算模型时需要用到
    time_series：时序分布，包含采样数据的时序分布情况
    splitter：分割映射关系
    """
    def __init__(self, model_name, time_series: TimeSeries, splitter: dict):
        self.model_name = model_name
        self.time_series = time_series
        self.splitter = splitter
        self.file_system = TensorflowSliceFileSystem(model_name=model_name)

    def do_select(self):
        for row in self.time_series:
            date = datetime.str_to_date(row['date'])
            if not self.already_exist(date=date):
                data = self.read(date=date, total=row['total'])
                start = 0
                for seg_type in self.splitter:
                    cnt = row[self.splitter[seg_type]]
                    self.file_system.write(
                        data=data.iloc[start:start+cnt, :],
                        date=date,
                        file_name=self.get_file_name(seg_type)
                    )
                    start += cnt

    def already_exist(self, date):
        for seg_type in self.splitter:
            file_name = self.get_file_name(seg_type)
            if not self.file_system.exist(date=date, file_name=file_name):
                return False
        return True

    """
    构造落地数据的文件名称
    """
    def get_file_name(self, key):
        return f'daily_slice_{key}.stock.gzip.pickle'

    """
    读取某日的股票数据，并从中随机选取total条数据返回
    date：选择数据的日期
    total：总共需要选出的数据条数
    """
    def read(self, date, total) -> DataFrame:
        all_records = ts_pro_bar.select_valid_record_by_date(date)
        select_indexes = random.sample(range(0, all_records.shape[0]), total)
        return all_records.iloc[select_indexes]


if __name__ == '__main__':
    time_series = TimeSeries(
        begin_date=datetime.str_to_date('20180320'),
        end_date=datetime.str_to_date('20200320'),
        total=40000,
        ratio=[0.5, ],
        distribution=uniform_distribution,
    )

    splitter = {
        'train': 'seg_0',
        'test': 'seg_1',
    }

    general_selection = Selection(
        model_name='general_model',
        time_series=time_series,
        splitter=splitter
    )
    general_selection.do_select()
