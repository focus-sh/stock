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
        for item in self.iterator:
            segment = item.segment
            result = pd.DataFrame({})
            for _, data in enumerate(item.data_list):
                result = pd.concat([result, data], ignore_index=True)
            self.file_system.write(data=result, segment=segment, file_name=self.target_file_name)


if __name__ == '__main__':
    executor
    merger = Merger(
        model_name='general_model',
        slice_key='date',
        file_name_list=['train.index.gzip.pickle', 'test.index.gzip.pickle'],
        target_file_name='merged.index.gzip.pickle',
    )

    merger.merge()
