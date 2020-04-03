import logging

from lib.datetime import datetime
from lib.executor import executor
from svc.train.classifier import Classifier
from svc.train.data_picker import select_valid_record, select_30d_up_50p_false_record, select_30d_up_50p_true_record
from svc.train.distribution import uniform_distribution
from svc.train.collector import Collector
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
        # 抽样数据起始时间
        self.begin_date = datetime.str_to_date('20190101')
        # 抽样数据结束时间
        self.end_date = datetime.str_to_date('20191231')

        self.sample_config = [
            {
                'id': '30d_up_50p_false',
                'total': 9000,
                'train_percent': 0.8,
                'distribution': uniform_distribution,
                'selector': select_30d_up_50p_false_record
            }, {
                'id': '30d_up_50p_true',
                'total': 1000,
                'train_percent': 0.8,
                'distribution': uniform_distribution,
                'selector': select_30d_up_50p_true_record
            }
        ]

    def generate_model_data(self):
        """
        生成计算模型时需要的数据
        """
        train_index_file_list, test_index_file_list = [], []
        logging.info('>>>从股票交易原始数据中根据数据分布函数按日期选取数据点')
        for _, config in enumerate(self.sample_config):
            time_series = TimeSeries(
                begin_date=self.begin_date,
                end_date=self.end_date,
                total=config['total'],
                train_percent=config['train_percent'],
                distribution=config['distribution'],
            )

            selection = Selection(
                id=config['id'],
                model_name=self.model_name,
                time_series=time_series,
                selector=config['selector'],
            )
            selection.do_select()
            train_index_file_list.append(selection.file_name_list[0])
            test_index_file_list.append(selection.file_name_list[1])

        logging.info('>>>将所有取样函数生成的文件汇总成日切形式的训练集数据索引和测试集数据索引')
        merger = Merger(
            model_name=self.model_name,
            slice_key='date',
            file_name_list=train_index_file_list,
            target_file_name='train.index.gzip.pickle',
        )
        merger.merge()

        merger = Merger(
            model_name=self.model_name,
            slice_key='date',
            file_name_list=test_index_file_list,
            target_file_name='test.index.gzip.pickle',
        )
        merger.merge()

        logging.info('>>>将日切的数据索引文件归并成汇总文件')
        collector = Collector(
            model_name=self.model_name,
            slice_key='date',
            file_name='train.index.gzip.pickle',
        )
        collector.collect()

        collector = Collector(
            model_name=self.model_name,
            slice_key='date',
            file_name='test.index.gzip.pickle',
        )
        collector.collect()

        logging.info('>>>将汇总的索引文件按股票代码进行拆分')
        classifier = Classifier(
            model_name=self.model_name,
            file_name='train.index.gzip.pickle',
            slice_key='stock',
            filter_key='ts_code',
        )
        classifier.classify()

        classifier = Classifier(
            model_name=self.model_name,
            file_name='test.index.gzip.pickle',
            slice_key='stock',
            filter_key='ts_code',
        )
        classifier.classify()

        logging.info('>>>使用股票基础信息填充样本数据')
        padding = Padding(
            model_name=self.model_name,
            index_file_name='train.index.gzip.pickle',
            sample_file_name='train.sample.gzip.pickle',
        )
        padding.fill()
        padding = Padding(
            model_name=self.model_name,
            index_file_name='test.index.gzip.pickle',
            sample_file_name='test.sample.gzip.pickle',
        )
        padding.fill()

        logging.info('>>>汇总所有股票样本数据')
        collector = Collector(
            model_name=self.model_name,
            slice_key='stock',
            file_name='train.sample.gzip.pickle',
        )
        collector.collect()

        collector = Collector(
            model_name=self.model_name,
            slice_key='stock',
            file_name='test.sample.gzip.pickle',
        )
        collector.collect()


model_30d_up_50p = Model30dUp50p()

if __name__ == '__main__':
    executor
    model_30d_up_50p.generate_model_data()
