import unittest

import numpy
import pandas


class TestPandasDataFrameMethods(unittest.TestCase):

    def test_create_data_frame(self):
        lst = ['Geek', 'For', 'Geeks', 'is', 'portal', 'for', 'Geeks']
        df = pandas.DataFrame(lst)
        print(df)
        self.assertIsNotNone(df)

        data = {
            'Name': ['Tom', 'nick', 'krish', 'jack'],
            'Age': [20, 21, 19, 18]
        }
        df = pandas.DataFrame(data)
        print(df)
        self.assertIsNotNone(df)

    def test_access_data_frame_by_column_name(self):
        data = {
            'Name': ['Jai', 'Princi', 'Gaurav', 'Anuj'],
            'Age': [27, 24, 22, 32],
            'Address': ['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
            'Qualification': ['Msc', 'MA', 'MCA', 'Phd']
        }
        df = pandas.DataFrame(data)
        print(df)

        sub_df = df[['Name', 'Qualification']]
        print(sub_df)
        self.assertIsNotNone(sub_df)

    def test_access_data_frame_by_row_selection(self):
        data = {
            'Name': ['Jai', 'Princi', 'Gaurav', 'Anuj'],
            'Age': [27, 24, 22, 32],
            'Address': ['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
            'Qualification': ['Msc', 'MA', 'MCA', 'Phd']
        }
        df = pandas.DataFrame(data)
        df.set_index('Name', inplace=True)

        first = df.loc['Jai']
        second = df.loc['Princi']
        print(first, '\n\n\n', second)
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)

    def test_select_data_frame_single_column(self):
        data = {
            'Name': ['Jai', 'Princi', 'Gaurav', 'Anuj'],
            'Age': [27, 24, 22, 32],
            'Address': ['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
            'Qualification': ['Msc', 'MA', 'MCA', 'Phd']
        }
        df = pandas.DataFrame(data)
        first = df['Name']
        print(first)
        self.assertIsNotNone(first)

    def test_select_data_frame_using_iloc(self):
        data = {
            'Name': ['Jai', 'Princi', 'Gaurav', 'Anuj'],
            'Age': [27, 24, 22, 32],
            'Address': ['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
            'Qualification': ['Msc', 'MA', 'MCA', 'Phd']
        }
        df = pandas.DataFrame(data)
        df.set_index('Name', inplace=True)

        row = df.iloc[0]
        print(row)

    def test_check_data_frame_missing_item(self):
        dict = {
            'First Score': [100, 90, numpy.nan, 95],
            'Second Score': [30, 45, 56, numpy.nan],
            'Third Score': [numpy.nan, 40, 80, 98]
        }

        df = pandas.DataFrame(dict)
        null_metrics = df.isnull()
        print(null_metrics)
        self.assertIsNotNone(null_metrics)

    def test_fill_data_frame_missing_item(self):
        dict = {
            'First Score': [100, 90, numpy.nan, 95],
            'Second Score': [30, 45, 56, numpy.nan],
            'Third Score': [numpy.nan, 40, 80, 98]
        }
        df = pandas.DataFrame(dict)

        df = df.fillna(0)
        print(df)
        self.assertIsNotNone(df)

    def test_drop_data_frame_missing_item(self):
        dict = {
            'First Score': [100, 90, numpy.nan, 95],
            'Second Score': [30, 45, 56, numpy.nan],
            'Third Score': [numpy.nan, 40, 80, 98]
        }
        df = pandas.DataFrame(dict)
        df = df.dropna()
        print(df)
        self.assertIsNotNone(df)

    def test_iterate_data_frame_rows(self):
        dict = {
            'name': ['aparna', 'pankaj', 'sudhir', 'Geeku'],
            'degree': ['MBA', 'BCA', 'M.Tech', 'MBA'],
            'score': [90, 40, 80, 98],
        }
        df = pandas.DataFrame(dict)

        for i, j in df.iterrows():
            print(i, j)
            print()

    def test_iterate_data_frame_column(self):
        dict = {
            'name': ['aparna', 'pankaj', 'sudhir', 'Geeku'],
            'degree': ['MBA', 'BCA', 'M.Tech', 'MBA'],
            'score': [90, 40, 80, 98],
        }
        df = pandas.DataFrame(dict)

        columns = list(df)
        for i in columns:
            print(df[i][2])

