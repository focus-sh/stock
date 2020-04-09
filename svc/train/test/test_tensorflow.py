import unittest
import tensorflow as tf
import pandas as pd
import numpy as np


class TestTensorflow(unittest.TestCase):
    @unittest.skip
    def test_tensorflow_data_structure(self):
        mnist = tf.keras.datasets.mnist
        (x_train, y_train), (x_test, y_test) = mnist.load_data()
        self.assertIsNotNone(x_train)
        self.assertIsNotNone(y_train)
        self.assertIsNotNone(x_test)
        self.assertIsNotNone(y_test)

        array_2d = []
        for index, item in enumerate(x_train):
            item_1d = item.flatten()
            item_1d = np.append(item_1d, y_train[index])
            array_2d.append(item_1d)
        train_data = pd.DataFrame(np.array(array_2d))

        train_file = 'mnist-train.gzip.pickle'
        train_data.to_pickle(train_file, compression="gzip")

        array_2d = []
        for index, item in enumerate(x_test):
            item_1d = item.flatten()
            item_1d = np.append(item_1d, y_test[index])
            array_2d.append(item_1d)
        test_data = pd.DataFrame(np.array(array_2d))

        test_file = 'mnist-test.gzip.pickle'
        test_data.to_pickle(test_file, compression="gzip")

        train_data_rd = pd.read_pickle(train_file, compression='gzip')
        test_data_rd = pd.read_pickle(test_file, compression="gzip")

        y_train = np.array(train_data_rd[train_data_rd.columns.values.max()].values)

        x_train = train_data_rd.drop([train_data_rd.columns.values.max()], axis=1)
        array_3d = []
        #for _, row in x_train.iterrows():
        #    item_2d = np.reshape(row.values, (-1, 28))
        #    array_3d.append(item_2d)
        #x_train = np.array(array_3d)
        x_train = np.array(x_train)

        y_test = np.array(test_data_rd[test_data_rd.columns.values.max()].values)

        x_test = test_data_rd.drop([test_data_rd.columns.values.max()], axis=1)
        #array_3d = []
        #for _, row in x_test.iterrows():
        #    item_2d = np.reshape(row.values, (-1, 28))
        #    array_3d.append(item_2d)
        #x_test = np.array(array_3d)
        x_test = np.array(x_test)

        x_train, x_test = x_train / 255.0, x_test / 255.0
        model = tf.keras.models.Sequential([
            tf.keras.layers.Flatten(input_shape=(784, )),
            tf.keras.layers.Dense(128, activation='relu'),
            tf.keras.layers.Dropout(0.2),
            tf.keras.layers.Dense(10, activation='softmax')
        ])

        model.compile(optimizer='adam',
                      loss='sparse_categorical_crossentropy',
                      metrics=['accuracy'])

        model.fit(x_train, y_train, epochs=5)
        model.evaluate(x_test, y_test)

    def test_model(self):
        train_file = '/Users/future/data/cache/tensorflow/model_30d_up_50p/total/model_30d_up_50p_train.sample.gzip.pickle'
        test_file = '/Users/future/data/cache/tensorflow/model_30d_up_50p/total/model_30d_up_50p_test.sample.gzip.pickle'

        train_data_rd = pd.read_pickle(train_file, compression='gzip')
        test_data_rd = pd.read_pickle(test_file, compression="gzip")

        y_train = np.array(train_data_rd[train_data_rd.columns.values.max()].values)

        x_train = train_data_rd.drop(['ts_code', 'trade_date', train_data_rd.columns.values.max()], axis=1)
        x_train = np.array(x_train)

        y_test = np.array(test_data_rd[test_data_rd.columns.values.max()].values)
        x_test = test_data_rd.drop(['ts_code', 'trade_date', test_data_rd.columns.values.max()], axis=1)

        x_test = np.array(x_test)

        model = tf.keras.models.Sequential([
            tf.keras.layers.Flatten(input_shape=(360, )),
            tf.keras.layers.Dense(128, activation='relu'),
            tf.keras.layers.Dropout(0.2),
            tf.keras.layers.Dense(64, activation='tanh')
        ])

        model.compile(optimizer='adam',
                      loss='sparse_categorical_crossentropy',
                      metrics=['accuracy'])

        model.fit(x_train, y_train, epochs=10)
        model.evaluate(x_test, y_test)
