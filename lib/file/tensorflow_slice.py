import os

from lib.executor import executor
from lib.file.tensorflow_model import TensorflowModelFileSystem
import pandas as pd

class FilesIterator:
    """
    分片文件遍历器
    file_system：文件系统
    file_name_list：需要查询的文件名称列表
    """
    def __init__(
            self,
            file_system,
            file_name_list,
    ):
        self.file_system = file_system
        self.file_name_list = file_name_list
        self.slices = []
        self.cur_index = -1

    class Slice:
        def __init__(
                self,
                segment,
                data_list
        ):
            self.segment = segment
            self.data_list = data_list

    def __iter__(self):
        self.slices = os.listdir(self.file_system.base_dir)
        self.cur_index = -1
        return self

    def __next__(self) :
        self.cur_index += 1
        if self.cur_index >= len(self.slices):
            raise StopIteration

        data_list = []
        for _, file_name in enumerate(self.file_name_list):
            segment = self.slices[self.cur_index]
            file_data = self.file_system.read(
                segment=segment,
                file_name=file_name
            )
            data_list.append(file_data)
        return self.Slice(segment, data_list)


class FileIterator:
    """
    分片文件遍历器
    file_system：文件系统
    file_name：文件名称
    """
    def __init__(
            self,
            file_system,
            file_name,
    ):
        self.file_system = file_system
        self.file_name = file_name
        self.slices = []
        self.cur_index = -1

    def __iter__(self):
        self.slices = os.listdir(self.file_system.base_dir)
        self.cur_index = -1
        return self

    def __next__(self) -> pd.DataFrame:
        self.cur_index += 1
        if self.cur_index >= len(self.slices):
            raise StopIteration

        return self.file_system.read(
            segment=self.slices[self.cur_index],
            file_name=self.file_name
        )


class TensorflowSliceFileSystem(TensorflowModelFileSystem):

    def __init__(self, model_name='comm', slice_key='date'):
        super(TensorflowSliceFileSystem, self).__init__(model_name=model_name)

        self.create_dir(slice_key)

        self.base_dir = self.base_dir + '/' + slice_key

    def get_file_name(self, segment, file_name) -> str:
        self.create_dir(segment)

        return f'{self.base_dir}/{segment}/{file_name}'

    def iterator(self, file_name) -> FileIterator:
        """
        生成可以遍历所有分片节点下数据的遍历器
        """
        return self.FileIterator(file_name)


file_system = TensorflowSliceFileSystem()


if __name__ == '__main__':
    executor
    file_system = TensorflowSliceFileSystem(model_name='general_model')
    iterator = FileIterator(file_system=file_system, file_name='train.index.gzip.pickle')
    for data in iterator:
        print(data)
