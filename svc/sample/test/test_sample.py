import datetime
import unittest

import tensorflow

from svc.sample.sample import Sample, uniform_distribution


class TestSample(unittest.TestCase):
    def test_do_service(self):
        sample = Sample(
            begin_date=datetime.date(2018, 3, 20),
            end_date=datetime.date(2020, 3, 20),
            sample_cnt=40000,
            ratio=0.9,
            distribution=uniform_distribution,
        )
        sample.do_service()


class TestUniformDistribution(unittest.TestCase):
    def test_get_probability(self):
        probability = uniform_distribution.probability(4, 4)
        self.assertEqual(probability, 0.2)
        probability = uniform_distribution.probability(0, 0)
        self.assertEqual(probability, 1)
