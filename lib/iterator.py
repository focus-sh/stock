import math

import pandas as pd


class Iterator:
    """
    遍历器，用于遍历特定对象
    target: 该对象需要支持count()和paged_select()方法，
      count()方法返回数据总条数
      paged_select()方法返回分页的数据
    """
    def __init__(self, target):
        self.batch_size = 100
        self.target = target
        count = target.count()
        self.end = int(math.ceil(float(count) / self.batch_size) * self.batch_size)

        # 当前游标位置
        self.curr_index = 0
        # 在分区数据内部的游标位置
        self.curr_slice_index = 0
        # 分区数据
        self.df_slice = pd.DataFrame({})

    def __iter__(self):
        self.load_slice()
        return self

    def __next__(self) -> pd.Series:
        if self.curr_slice_index >= self.df_slice.shape[0]:
            if self.curr_index >= self.end:
                raise StopIteration
            else:
                self.load_slice()

        result = self.df_slice.iloc[self.curr_slice_index]
        self.curr_slice_index += 1
        return result

    def load_slice(self):
        self.df_slice = self.target.paged_select(self.curr_index, self.batch_size)
        self.curr_index += self.batch_size
        self.curr_slice_index = 0
