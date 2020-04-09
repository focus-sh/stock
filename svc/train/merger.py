from lib.executor import executor
from lib.file.tensorflow_slice import FilesIterator, TensorflowSliceFileSystem
import pandas as pd


class Merger:
    """
    文件归并器，将同一类型的不同文件归并成一个文件
    model_name：模型名称，用于定位分片数据位置，并将生成的数据存储在相应的位置中
    slice_key：分片类型，用于定位分片数据位置
    file_name_list：需要归并的文件名称列表
    target_file_name：归并数据保存的文件名称
    """
    def __init__(
            self,
            model_name,
            slice_key,
            file_name_list,
            target_file_name
    ):
        self.target_file_name = target_file_name
        self.file_system = TensorflowSliceFileSystem(model_name=model_name, slice_key=slice_key)
        self.iterator = FilesIterator(file_system=self.file_system, file_name_list=file_name_list)

    def merge(self):
        """
        归并所有文件数据
        """
        for segment, content in self.iterator:
            if not self.file_system.exist(segment=segment, file_name=self.target_file_name):
                result = pd.DataFrame({})
                for _, data in enumerate(content):
                    result = pd.concat([result, data], ignore_index=True)
                self.file_system.write(data=result, segment=segment, file_name=self.target_file_name)


if __name__ == '__main__':
    executor
    merger = Merger(
        model_name='general_model',
        slice_key='date',
        file_name_list=['30d_up_50p_false.index.gzip.pickle', '30d_up_50p_true.index.gzip.pickle'],
        target_file_name='30d_up_50p.index.gzip.pickle',
    )

    merger.merge()

    file_system = TensorflowSliceFileSystem(model_name='general_model')
    data = file_system.read(segment='20180102', file_name='30d_up_50p.index.gzip.pickle')
    print(data)
