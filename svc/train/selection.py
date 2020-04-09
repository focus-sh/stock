import logging

from lib.datetime import datetime
from lib.executor import executor
from lib.file.tensorflow_slice import TensorflowSliceFileSystem
from svc.train.data_picker import select_30d_up_50p_true_record, select_30d_up_50p_false_record
from svc.train.distribution import uniform_distribution
from svc.train.time_series import TimeSeries


class Selection:
    """
    取样器，根据时序分布数据、缓存的每日股票清单数据生成日切的样本清单
    id：取样器的唯一标示，用于区分每个取样器生成的文件
    model_name：模型名称，用于确定生成的日切文件的存储位置，后续计算模型时需要用到
    time_series：时序分布，包含采样数据的时序分布情况
    splitter：分割映射关系
    """
    def __init__(
            self,
            id: str,
            model_name: str,
            time_series: TimeSeries,
            selector,
    ):
        self.id = id
        self.file_system = TensorflowSliceFileSystem(model_name=model_name)
        self.file_name = f'{self.id}.index.gzip.pickle'
        self.time_series = time_series
        self.selector = selector

    def do_select(self):
        # 用于记录当前数据的差值（比如某个时间点的数据不足的时候，则将差值数据保存下来，尝试从后面数据中不足
        total_debt = 0
        for row in self.time_series:
            date = datetime.str_to_date(row['date'])
            if not self.file_system.exist(segment=datetime.date_to_str(date), file_name=self.file_name):
                # 需要获取的数为分布函数中要求的总数加上到目前为止还欠下的差值
                required_total_cnt = row['total'] + total_debt
                data = self.selector(date=date, total=required_total_cnt)

                actual_total_cnt = data.shape[0]

                # write to training file
                self.file_system.write(
                    data=data,
                    segment=datetime.date_to_str(date),
                    file_name=self.file_name
                )
                # 更新还欠的数据
                total_debt = required_total_cnt - actual_total_cnt
        if total_debt > 0:
            logging.warning(f"抽样数据不足，总计数据差值为，total_debt={total_debt}")


if __name__ == '__main__':
    executor
    time_series = TimeSeries(
        begin_date=datetime.str_to_date('20180101'),
        end_date=datetime.str_to_date('20180331'),
        total=500,
        distribution=uniform_distribution,
    )

    general_selection = Selection(
        id='30d_up_50p_false',
        model_name='general_model',
        time_series=time_series,
        selector=select_30d_up_50p_false_record,
    )
    general_selection.do_select()

    general_selection = Selection(
        id='30d_up_50p_true',
        model_name='general_model',
        time_series=time_series,
        selector=select_30d_up_50p_true_record,
    )
    general_selection.do_select()

    file_system = TensorflowSliceFileSystem(model_name='general_model')
    index_data = file_system.read(segment='20180102', file_name='30d_up_50p_false.index.gzip.pickle')
    print(index_data)

    index_data = file_system.read(segment='20180102', file_name='30d_up_50p_true.index.gzip.pickle')
    print(index_data)
