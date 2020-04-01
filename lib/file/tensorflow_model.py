from lib.file.tensorflow import TensorflowFileSystem


class TensorflowModelFileSystem(TensorflowFileSystem):
    def __init__(self, model_name='comm'):
        super(TensorflowModelFileSystem, self).__init__()

        self.model_name = model_name
        self.create_dir(self.model_name)

        self.base_dir = self.base_dir + '/' + self.model_name


file_system = TensorflowModelFileSystem(model_name='comm')
