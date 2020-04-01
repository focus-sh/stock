from lib.file.tensorflow_model import TensorflowModelFileSystem


class TensorflowSliceFileSystem(TensorflowModelFileSystem):
    def __init__(self, model_name='comm'):
        super(TensorflowSliceFileSystem, self).__init__(model_name=model_name)

        self.relative_path = 'slice'
        self.create_dir(self.relative_path)

        self.base_dir = self.base_dir + '/' + self.relative_path

    def get_file_name(self, date, file_name) -> str:
        relative_path = date.strftime("%Y%m%d")
        self.create_dir(relative_path)

        return f'{self.base_dir}/{relative_path}/{file_name}'


file_system = TensorflowSliceFileSystem()
