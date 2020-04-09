from lib.executor import executor
from lib.file.tensorflow_slice import TensorflowSliceFileSystem
from lib.file.tensorflow_total import TensorflowTotalFileSystem


class Classifier:
    """
    分类器，将文件内容根据指定维度进行分类
    model_name：模型名称，用于定位文件位置
    file_name：处理的文件名称，分类器默认处理的文件是total（汇总）的文件
    slice_key：分类文件存储的位置
    filter_key：用于对汇总数据进行分类的指标
    """
    def __init__(
            self,
            model_name,
            file_name,
            slice_key,
            filter_key,
    ):
        self.classifier_file_system = TensorflowSliceFileSystem(model_name=model_name, slice_key=slice_key)
        self.total_file_system = TensorflowTotalFileSystem(model_name=model_name)
        self.file_name = file_name
        self.filter_key = filter_key

    def classify(self):
        total_data = self.total_file_system.read(file_name=self.file_name)
        classification = total_data[self.filter_key].unique()
        for filter_value in classification:
            if not self.classifier_file_system.exist(segment=filter_value, file_name=self.file_name):
                stock_data = total_data[total_data[self.filter_key].isin([filter_value])].copy()
                stock_data = stock_data.sort_values(by='trade_date').reset_index(drop=True)
                self.classifier_file_system.write(data=stock_data, segment=filter_value, file_name=self.file_name)


if __name__ == '__main__':
    executor
    classifier = Classifier(
        model_name='general_model',
        file_name='30d_up_50p.index.gzip.pickle',
        slice_key='stock',
        filter_key='ts_code',
    )
    classifier.classify()

    file_system = TensorflowSliceFileSystem(model_name='general_model', slice_key='stock')
    data = file_system.read(segment='002137.SZ', file_name='30d_up_50p.index.gzip.pickle')
    print(data)


