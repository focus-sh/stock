import unittest

from libs.common import format_value


class TestCommonMethods(unittest.TestCase):

    def test_can_format_float_num(self):
        val = format_value('0.2341242523535')
        self.assertEqual(val, '0.2341')

    def test_can_format_None(self):
        self.assertIsNone(format_value(None))

    def test_can_format_str(self):
        self.assertEqual(format_value("hello world"), "hello world")
