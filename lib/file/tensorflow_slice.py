import os

from lib.executor import executor
from lib.file.tensorflow_model import TensorflowModelFileSystem
import pandas as pd


class BaseIterator:
    """
    分片遍历器
    """
    def __init__(
            self,
            file_system
    ):
        self.file_system = file_system
        self.slices = []
        self.cur_index = -1

    def __iter__(self):
        """
        初始化文件遍历器，删除其中的.DS_Store文件夹（该文件是MacOS的系统文件）
        :return:
        """
        self.slices = os.listdir(self.file_system.base_dir)
        if '.DS_Store' in self.slices:
            self.slices.remove('.DS_Store')
        self.cur_index = -1
        return self


class FileIterator(BaseIterator):
    """
    单个嗯见的分片遍历器
    file_system：文件系统
    file_name：文件名称
    """
    def __init__(
            self,
            file_system,
            file_name,
    ):
        super(FileIterator, self).__init__(file_system=file_system)
        self.file_name = file_name

    def __next__(self) -> pd.DataFrame:
        self.cur_index += 1
        if self.cur_index >= len(self.slices):
            raise StopIteration

        segment = self.slices[self.cur_index]
        content = self.file_system.read(
            segment=segment,
            file_name=self.file_name
        )

        return segment, content


class FilesIterator(BaseIterator):
    """
    多文件的分片遍历器
    file_system：文件系统
    file_name_list：需要查询的文件名称列表
    """
    def __init__(
            self,
            file_system,
            file_name_list,
    ):
        super(FilesIterator, self).__init__(file_system=file_system)
        self.file_name_list = file_name_list

    def __next__(self):
        self.cur_index += 1
        if self.cur_index >= len(self.slices):
            raise StopIteration

        content = []
        for _, file_name in enumerate(self.file_name_list):
            segment = self.slices[self.cur_index]
            file_data = self.file_system.read(
                segment=segment,
                file_name=file_name
            )
            content.append(file_data)
        return segment, content


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
    data = file_system.read(segment='20180320', file_name='NO SUCH FIILE')
    print(data)
