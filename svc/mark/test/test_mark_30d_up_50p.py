import unittest

from svc.mark.mark_30d_up_50p import mark_30d_up_50p


class TestMark30dUp50p(unittest.TestCase):

    def test_mark_30d_up_50p(self):
        mark_30d_up_50p.run()
