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
        result = pd.DataFrame({})
        for slice_data in self.iterator:
            result = pd.concat([result, slice_data], ignore_index=True)

        self.total_file_system.write(data=result, file_name=self.file_name)


if __name__ == '__main__':
    executor
    collector = Collector(
        model_name='general_model',
        slice_key='date',
        file_name='train.index.gzip.pickle',
    )

    collector = Collector(
        model_name='general_model',
        slice_key='stock',
        file_name='train.sample.gzip.pickle',
    )
    collector.collect()

    total_file_system = TensorflowTotalFileSystem(model_name='general_model')
    data = total_file_system.read(file_name='train.sample.gzip.pickle')
    print(data.head(100))
