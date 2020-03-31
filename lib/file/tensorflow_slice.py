from lib.file.tensorflow_model import TensorflowModelFileSystem


class TensorflowSliceFileSystem(TensorflowModelFileSystem):
    def __init__(self, model_name='comm'):
        super(TensorflowSliceFileSystem, self).__init__(model_name=model_name)
        self.base_dir = self.base_dir + '/slice'
        self.init_root()

    def get_file_name(self, date, file_name) -> str:
        relative_path = date.strftime("%Y%m%d")
        self.create_dir(relative_path)
        return f'{self.base_dir}/{relative_path}/{file_name}'


file_system = TensorflowSliceFileSystem()
