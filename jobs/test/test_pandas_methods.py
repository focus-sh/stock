import sys
import unittest

import pandas
from sqlalchemy.exc import ProgrammingError

from libs import common


class TestPandasMethods(unittest.TestCase):

    def test_read_sql_func(self):
        with self.assertRaises(ProgrammingError):
            pandas.read_sql('select * from no_such_table', common.engine())
