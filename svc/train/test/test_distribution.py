import random
import unittest

from svc.train.distribution import uniform_distribution


class TestUniformDistribution(unittest.TestCase):
    def test_sample_from_range(self):
        data = random.sample(range(0, 100), 20)
        self.assertIsNotNone(data)

    def test_get_probability(self):
        probability = uniform_distribution.probability(4, 4)
        self.assertEqual(probability, 0.2)
        probability = uniform_distribution.probability(0, 0)
        self.assertEqual(probability, 1)
