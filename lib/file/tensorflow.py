from lib.file_system import FileSystem


class TensorflowFileSystem(FileSystem):
    def __init__(self):
        super(TensorflowFileSystem, self).__init__()

        self.relative_path = 'tensorflow'
        self.create_dir(self.relative_path)

        self.base_dir = self.base_dir + '/' + self.relative_path


file_system = TensorflowFileSystem()
