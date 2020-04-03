from lib.executor import executor
from lib.file.tensorflow_slice import TensorflowSliceFileSystem
from lib.iterator import Iterator
from model.pro_stock_basic import pro_stock_basic
from model.ts_pro_bar import ts_pro_bar
import pandas as pd


class Padding:
    """
    模型数据填充器
    model_name：数据填充器隶属的模型名称，用于查询抽样数据存储位置，并将生成的数据存储在相应的位置中
    index_file_name：抽样数据的索引文件，包含<ts_code,trade_date>，结合model_name确定抽样数据的存储位置
    sample_file_name：填充后的样本数据，格式为<ts_code, trade_date, x0, x1, ..., xn, y>
    """
    def __init__(
            self,
            model_name,
            index_file_name,
            sample_file_name,
    ):
        self.slice_file_system = TensorflowSliceFileSystem(model_name=model_name, slice_key='stock')
        self.model_name = model_name
        self.index_file_name = index_file_name
        self.sample_file_name = sample_file_name

    def fill(self):
        for row in Iterator(pro_stock_basic):
            ts_code = row['ts_code']
            if self.slice_file_system.exist(segment=ts_code, file_name=self.index_file_name):
                self.sample(ts_code)

    def sample(self, ts_code):
        if not self.slice_file_system.exist(segment=ts_code, file_name=self.sample_file_name):
            stock_data = ts_pro_bar.select_all_valid_record(ts_code)
            index_data = self.slice_file_system.read(segment=ts_code, file_name=self.index_file_name)
            #  默认取当天open（开盘价）、high（最高价）、low（最低价）、close（收盘价）、vol（成交量）、amount（成交额）
            result = pd.DataFrame({})
            for _, row in index_data.iterrows():
                stock = stock_data.loc[stock_data['trade_date'] == row['trade_date']].iloc[0]
                sample_row = row.copy()
                sample_row['x0'] = stock['open']
                sample_row['x1'] = stock['high']
                sample_row['x2'] = stock['low']
                sample_row['x3'] = stock['close']
                sample_row['x4'] = stock['vol']
                sample_row['x5'] = stock['amount']
                sample_row['y'] = 0
                result = result.append(sample_row, ignore_index=True)

            self.slice_file_system.write(data=result, segment=ts_code, file_name=self.sample_file_name)


if __name__ == '__main__':
    executor
    padding = Padding(
        model_name='general_model',
        index_file_name='train.index.gzip.pickle',
        sample_file_name='train.sample.gzip.pickle',
    )
    padding.fill()

