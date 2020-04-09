import logging

from lib.datetime import datetime
from lib.executor import executor
from svc.train.classifier import Classifier
from svc.train.collector import Collector
from svc.train.data_picker import select_30d_up_50p_false_record, select_30d_up_50p_true_record
from svc.train.distribution import uniform_distribution
from svc.train.fill import fill_30d_up_50p
from svc.train.merger import Merger
from svc.train.padding import Padding
from svc.train.selection import Selection
from svc.train.time_series import TimeSeries


class Model30dUp50p:
    """
    30天内股票上涨50%的选股策略模型
    """
    def __init__(self):
        # 模型名称
        self.model_name = 'model_30d_up_50p'
        self.index_file_name = f'{self.model_name}.index.gzip.pickle'
        self.sample_file_name = f'{self.model_name}.sample.gzip.pickle'

        # 抽样数据起始时间
        self.begin_date = datetime.str_to_date('20080101')
        # 抽样数据结束时间
        self.end_date = datetime.str_to_date('20191231')

        self.sample_config = [
            {
                'id': '30d_up_50p_false',
                'total': 60000,
                'distribution': uniform_distribution,
                'selector': select_30d_up_50p_false_record
            }, {
                'id': '30d_up_50p_true',
                'total': 40000,
                'distribution': uniform_distribution,
                'selector': select_30d_up_50p_true_record
            }
        ]

        self.filling_fn = fill_30d_up_50p

    def generate_model_data(self):
        """
        生成计算模型时需要的数据
        """
        index_file_list = []
        logging.info('>>>从股票交易原始数据中根据数据分布函数按日期选取数据点')
        for _, config in enumerate(self.sample_config):
            time_series = TimeSeries(
                begin_date=self.begin_date,
                end_date=self.end_date,
                total=config['total'],
                distribution=config['distribution'],
            )

            selection = Selection(
                id=config['id'],
                model_name=self.model_name,
                time_series=time_series,
                selector=config['selector'],
            )
            selection.do_select()
            index_file_list.append(selection.file_name)

        logging.info('>>>按时间切片汇总各分量数据至索引文件中')
        merger = Merger(
            model_name=self.model_name,
            slice_key='date',
            file_name_list=index_file_list,
            target_file_name=self.index_file_name,
        )
        merger.merge()

        logging.info('>>>将日切的索引文件归并为总索引文件')
        collector = Collector(
            model_name=self.model_name,
            slice_key='date',
            file_name=self.index_file_name,
        )
        collector.collect()

        logging.info('>>>将汇总的索引文件按股票代码进行拆分')
        classifier = Classifier(
            model_name=self.model_name,
            file_name=self.index_file_name,
            slice_key='stock',
            filter_key='ts_code',
        )
        classifier.classify()

        logging.info('>>>使用股票基础信息填充样本数据')
        padding = Padding(
            model_name=self.model_name,
            index_file_name=self.index_file_name,
            sample_file_name=self.sample_file_name,
            fn=self.filling_fn,
        )
        padding.fill()

        logging.info('>>>汇总所有股票样本数据')
        collector = Collector(
            model_name=self.model_name,
            slice_key='stock',
            file_name=self.sample_file_name,
        )
        collector.collect()


model_30d_up_50p = Model30dUp50p()

if __name__ == '__main__':
    executor
    model_30d_up_50p.generate_model_data()
