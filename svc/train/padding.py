import logging
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

from lib.executor import executor
from lib.file.tensorflow_slice import TensorflowSliceFileSystem, FileIterator
from svc.train.fill import fill_30d_up_50p


class Padding:
    """
    模型数据填充器
    model_name：数据填充器隶属的模型名称，用于查询抽样数据存储位置，并将生成的数据存储在相应的位置中
    index_file_name：抽样数据的索引文件，包含<ts_code,trade_date>，结合model_name确定抽样数据的存储位置
    sample_file_name：填充后的样本数据，格式为<ts_code, trade_date, x0, x1, ..., xn, y>
    fn：填充函数fn(str, list) -> DataFrame
    thread_cnt：线程数
    """
    def __init__(
            self,
            model_name,
            index_file_name,
            sample_file_name,
            fn,
            thread_cnt=4,
    ):
        self.model_name = model_name
        self.index_file_name = index_file_name
        self.sample_file_name = sample_file_name

        self.slice_file_system = TensorflowSliceFileSystem(model_name=model_name, slice_key='stock')
        self.iterator = FileIterator(self.slice_file_system, self.index_file_name)

        self.filling = fn
        self.executors = ThreadPoolExecutor(max_workers=thread_cnt)

    def fill(self):
        task_list = []
        for ts_code, content in self.iterator:
            if not content.empty:
                date_list = content['trade_date'].to_list()
                if not self.slice_file_system.exist(segment=ts_code, file_name=self.sample_file_name):
                    task_list.append(self.executors.submit(
                        self.filling,
                        ts_code=ts_code,
                        date_list=date_list
                    ))

        for task in as_completed(task_list):
            ts_code, data = task.result()
            logging.info(f'Saving slice sample data of {ts_code}')
            self.slice_file_system.write(data=data, segment=ts_code, file_name=self.sample_file_name)


if __name__ == '__main__':
    executor
    padding = Padding(
        model_name='general_model',
        index_file_name='30d_up_50p.index.gzip.pickle',
        sample_file_name='30d_up_50p.sample.gzip.pickle',
        fn=fill_30d_up_50p,
    )
    padding.fill()

