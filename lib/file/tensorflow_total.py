from lib.file.tensorflow_model import TensorflowModelFileSystem


class TensorflowTotalFileSystem(TensorflowModelFileSystem):
    def __init__(self, model_name='comm'):
        super(TensorflowTotalFileSystem, self).__init__(model_name=model_name)

        self.relative_path = 'total'
        self.create_dir(self.relative_path)

        self.base_dir = self.base_dir + '/' + self.relative_path


    def get_file_name(self, file_name) -> str:
        return f'{self.base_dir}/{file_name}'


file_system = TensorflowTotalFileSystem()
