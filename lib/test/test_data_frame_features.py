import os
import unittest
import pandas as pd
import numpy as np


class TestDataFrameFeatures(unittest.TestCase):

    def test_can_remove_duplicated_records(self):
        data = {
            'name': ["KING", "KING"],
            'age': [12, 13],
        }
        df = pd.DataFrame(data)
        df = df.drop_duplicates(subset='name', keep='last')
        self.assertEqual(df.size, 2)
        self.assertEqual(df.iat[0, 1], 13)

    def test_data_frame_index(self):
        data = {
            'name': ["KING", "KING"],
            'age': [12, 13],
        }
        df = pd.DataFrame(data)
        np.testing.assert_almost_equal(df.index.values, [0, 1])

    def test_data_frame_column_type(self):
        data = {
            'name': ["KING", "KING"],
            'age': [12, 13],
        }
        df = pd.DataFrame(data)
        self.assertIsInstance(df['name'], pd.Series)

    def test_can_add_Series_to_dict(self):
        target = {}
        data = {
            'name': ["KING", "KING"],
            'age': [12, 13],
        }
        df = pd.DataFrame(data)
        target['name'] = df['name']
        self.assertIsInstance(target['name'], pd.Series)

    def test_can_create_data_frame_using_Series_dict(self):
        target = {}
        data = {
            'name': ["KING", "KING"],
            'age': [12, 13],
            'sex': ['M', 'F']
        }
        df = pd.DataFrame(data)
        target['name'] = df['name']
        target['age'] = df['age']

        result = pd.DataFrame(target, df.index.values)
        self.assertEqual(result.size, 4)

    def test_data_frame_apply_method(self):
        data = {
            'name': ["KING", "KING"],
            'age': [12, 13],
            'sex': ['M', 'F']
        }
        df = pd.DataFrame(data)

        def become_old(tmp):
            tmp['age'] = tmp['age'] + 1
            return tmp

        result = df.apply(become_old, axis=1)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(result.size, 6)

    def test_data_frame_to_pickle_method(self):
        try:
            data = {
                'name': ["KING", "KING"],
                'age': [12, 13],
                'sex': ['M', 'F']
            }
            df = pd.DataFrame(data)
            df.to_pickle('./dummy.pkl')
            df2 = pd.read_pickle('./dummy.pkl')
            self.assertTrue(df.equals(df2))
        finally:
            os.remove('./dummy.pkl')
