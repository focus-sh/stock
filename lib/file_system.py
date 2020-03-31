import logging
import os
import shutil
import pandas as pd
from pandas import DataFrame

from lib.environment import environment as env


class FileSystem:
    """
    本地文件系统，FileSystem是基础类，根据不同的使用场景，继承该类并实现相应功能
    """
    def __init__(self):
        #  默认的根目录是～/data/cache，base是用于测试FileSystem使用的基础目录
        self.base_dir = env.home() + "/data/cache/base"
        self.init_root()

        #  默认的压缩方法
        self.compression = "gzip"

    def init_root(self):
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir, exist_ok=True)

    def clear(self):
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)

        #  创建一个空的根目录
        self.init_root()

    def create_dir(self, relative_path: str) -> None:
        absolute_path = self.base_dir + '/' + relative_path
        if not os.path.exists(absolute_path):
            os.makedirs(absolute_path, exist_ok=True)

    def write(self, data: DataFrame, *args, **kwargs) -> None:
        file_name = self.get_file_name(args, kwargs)
        data.to_pickle(file_name, compression=self.compression)

    def delete(self, *args, **kwargs) -> None:
        """
        根据参数，删除对应的文件
        """
        file_name = self.get_file_name(args, kwargs)
        if os.path.exists(file_name):
            os.remove(file_name)

    def read(self, *args, **kwargs) -> DataFrame:
        file_name = self.get_file_name(args, kwargs)
        if os.path.exists(file_name):
            return pd.read_pickle(file_name, compression=self.compression)
        return None

    def get_file_name(self, *args, **kwargs) -> str:
        """
        根据传入参数，获取文件名称
        """
        pass

    # 用于初始化文件系统
    def init(self, *args, **kwargs) -> None:
        pass


file_system = FileSystem()