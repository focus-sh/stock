import unittest

from lib.file.tensorflow import file_system


class TestTensorflow(unittest.TestCase):

    def test_setup(self):
        self.assertTrue('tensorflow' in file_system.base_dir)
