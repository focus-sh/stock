import datetime
import unittest
import random

from lib.executor import executor
from svc.sample.distribution import uniform_distribution
from svc.sample.sample import Sample


class TestSample(unittest.TestCase):
    @unittest.skip
    def test_sample_from_range(self):
        data = random.sample(range(0, 100), 20)
        self.assertIsNotNone(data)

    @unittest.skip
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
