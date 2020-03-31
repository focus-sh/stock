from lib.file_system import FileSystem


class TensorflowFileSystem(FileSystem):
    def __init__(self):
        super(TensorflowFileSystem, self).__init__()
        self.base_dir = self.base_dir + '/tensorflow'
        self.init_root()


file_system = TensorflowFileSystem()
