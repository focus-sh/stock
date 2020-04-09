import random

import pandas as pd

from lib.executor import executor
from lib.file.tensorflow_slice import TensorflowSliceFileSystem, FileIterator
from lib.file.tensorflow_total import TensorflowTotalFileSystem


class Collector:
    """
    数据汇总器，将分片数据归并成一个大文件
    model_name：模型名称，用于定位分片数据位置，并将生成的数据存储在相应的位置中
    slice_key：分片类型，用于定位分片数据位置
    file_name：处理的文件名称，该文件名称既是分片数据的名称，也是合并后数据的名称（存储的路径不同）
    """
    def __init__(
            self,
            model_name,
            slice_key,
            file_name,
    ):
        self.file_name = file_name
        self.slice_file_system = TensorflowSliceFileSystem(model_name=model_name, slice_key=slice_key)
        self.iterator = FileIterator(self.slice_file_system, self.file_name)
        self.total_file_system = TensorflowTotalFileSystem(model_name=model_name)

    def collect(self):
        if not self.total_file_system.exist(file_name=self.file_name):
            result = pd.DataFrame({})
            for _, slice_data in self.iterator:
                if slice_data is not None:
                    result = pd.concat([result, slice_data], ignore_index=True)

            self.total_file_system.write(data=result, file_name=self.file_name)


if __name__ == '__main__':
    executor
    total_file_system = TensorflowTotalFileSystem(model_name='model_30d_up_50p')
    data = total_file_system.read(file_name='model_30d_up_50p.sample.gzip.pickle')

    ratio = 0.9
    train_indexes = []
    test_indexes = []
    for i in range(0, len(data.index)):
        if random.random() < 0.9:
            train_indexes.append(i)
        else:
            test_indexes.append(i)

    train_data = data.iloc[train_indexes, :].copy().reset_index(drop=True)
    test_data = data.iloc[test_indexes, :].copy().reset_index(drop=True)

    total_file_system.write(data=train_data, file_name='model_30d_up_50p_train.sample.gzip.pickle')
    total_file_system.write(data=test_data, file_name='model_30d_up_50p_test.sample.gzip.pickle')
