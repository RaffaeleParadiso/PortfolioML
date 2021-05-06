"""Unit test split.py"""
import os
import random
import unittest

import numpy as np
import pandas as pd
from portfolioML.makedir import go_up
from portfolioML.model.split import (all_data_LSTM, get_sequences,
                                     get_train_set, split_Tperiod)


def _full_path(file_name):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), file_name)


df_return = pd.read_csv(_full_path(
    go_up(1) + f'/PortfolioML/portfolioML/data/ReturnsData.csv'))

df_binary = pd.read_csv(_full_path(
    go_up(1) + f'/PortfolioML/portfolioML/data/ReturnsBinary.csv'))


class TestDataReturns(unittest.TestCase):

    def test_Tperiod(self):
        """Test of Tperiod fuction, each study period must have the same lenght"""
        per_ret, per_bin = split_Tperiod(df_return, df_binary)
        check_value = len(per_ret[0])
        for elm1, elm2 in zip(per_ret, per_bin):
            self.assertAlmostEqual(check_value, len(elm1))
            self.assertAlmostEqual(check_value, len(elm2))

    def test_get_sequences(self):
        """Test of split_sequences fuction"""
        self.assertEqual(len(df_return), len(df_binary))
        column_idx = random.randint(1, df_return.shape[1])
        random_tick = df_binary.columns[column_idx]
        X, y = get_sequences(df_return[random_tick], df_binary[random_tick])
        for i in range(1, 10):
            self.assertAlmostEqual(X[i][0], X[i - 1][1])

    def test_get_train_set(self):
        """
        Test of get_train_set fuction.
        The check is maded compare the result of np.stack and the results
        of get_trai_set when we use the reshape of numpy.
        """

        # nstack
        list1 = []
        for col in df_return.columns[:9]:
            x1, y1 = get_sequences(df_return[col], df_binary[col])
            list1.append(x1)
        list1 = np.array(list1)
        list1 = list((list1[i] for i in range(list1.shape[0])))
        list1 = np.vstack(list1)

        # reshape
        list2, y2 = get_train_set(df_return, df_binary)
        self.assertTrue((list1 == list2[:9 * 6306]).all())

    def test_all_data_LSTM(self):
        '''Test of all_data_LSTM function'''
        len_train = np.arange(500, 1000, 100)
        for train in len_train:
            X_train, y_train, X_test, y_test = all_data_LSTM(
                df_return, df_binary, period=7, len_train=train)
            self.assertEqual(X_train.shape[0], y_train.shape[0])
            self.assertEqual(X_test.shape[0], y_test.shape[0])


if __name__ == '__main__':
    unittest.main()
