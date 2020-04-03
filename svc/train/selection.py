import logging

from lib.datetime import datetime
from lib.executor import executor
from lib.file.tensorflow_slice import TensorflowSliceFileSystem
from svc.train.data_picker import select_valid_record, select_30d_up_50p_true_record
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
        self.file_name_list = [f'{self.id}_train.index.gzip.pickle', f'{self.id}_test.index.gzip.pickle']
        self.time_series = time_series
        self.selector = selector

    def do_select(self):
        # 用于记录当前数据的差值（比如某个时间点的数据不足的时候，则将差值数据保存下来，尝试从后面数据中不足
        total_debt, train_debt, test_debt = 0, 0, 0
        for row in self.time_series:
            date = datetime.str_to_date(row['date'])
            if not self.already_exist(date=date):
                # 需要获取的数为分布函数中要求的总数加上到目前为止还欠下的差值
                required_total_cnt = row['total'] + total_debt
                required_train_cnt = row['train'] + train_debt
                required_test_cnt = row['test'] + test_debt
                data = self.selector(date=date, total=required_total_cnt)

                actual_total_cnt = data.shape[0]

                (actual_train_cnt, actual_test_cnt) = self.redistribute(
                    actual_total_cnt=actual_total_cnt,
                    required_train_cnt=required_train_cnt,
                    required_test_cnt=required_test_cnt
                )

                # write to training file
                self.file_system.write(
                    data=data.iloc[0:actual_train_cnt, :],
                    segment=datetime.date_to_str(date),
                    file_name=self.file_name_list[0]
                )
                # write to test file
                self.file_system.write(
                    data=data.iloc[actual_train_cnt:actual_total_cnt, :],
                    segment=datetime.date_to_str(date),
                    file_name=self.file_name_list[1]
                )

                # 更新还欠的数据
                total_debt = required_total_cnt - actual_total_cnt
                train_debt = required_train_cnt - actual_train_cnt
                test_debt = required_test_cnt - actual_test_cnt
        if total_debt > 0:
            logging.warning(f"抽样数据不足，总计数据差值为，total_debt={total_debt}, train_debt={train_debt}, test_debt={test_debt}")

    def already_exist(self, date):
        for _, file_name in enumerate(self.file_name_list):
            if not self.file_system.exist(segment=datetime.date_to_str(date), file_name=file_name):
                return False
            return True

    def redistribute(self, actual_total_cnt, required_train_cnt, required_test_cnt):
        """
        将实际得到的数据，根据原来train/test的分配比例，重新分配
        """
        if actual_total_cnt >= required_train_cnt + required_test_cnt:
            return required_train_cnt, required_test_cnt # 数据充足，直接返回

        ratio = required_train_cnt / (required_train_cnt + required_test_cnt)
        return int(actual_total_cnt * ratio), actual_total_cnt - int(actual_total_cnt * ratio)

    """
    构造落地数据的文件名称
    """
    def get_file_name(self, key):
        return f'{key}.index.gzip.pickle'


if __name__ == '__main__':
    executor
    time_series = TimeSeries(
        begin_date=datetime.str_to_date('20180320'),
        end_date=datetime.str_to_date('20200320'),
        total=10000,
        train_percent=0.8,
        distribution=uniform_distribution,
    )

    general_selection = Selection(
        id='30d_up_50p',
        model_name='general_model',
        time_series=time_series,
        selector=select_valid_record,
    )
    general_selection.do_select()



