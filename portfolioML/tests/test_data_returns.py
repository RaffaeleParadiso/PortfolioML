"""Unit test data_returns"""
import os
import random
import unittest

import pandas as pd
from portfolioML.data.data_returns import binary_targets, get_returns
from portfolioML.makedir import go_up


def _full_path(file_name):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), file_name)


dataframe = pd.read_csv(_full_path(
    go_up(1) + f'/PortfolioML/portfolioML/data/PriceData.csv'))


class TestDataReturns(unittest.TestCase):
    """Class test for data_returns.py"""

    def test_m(self):
        """
        Test correct result with some value m
        """
        m_list = range(1, 10)
        for m in m_list:
            self.assertAlmostEqual(get_returns(
                dataframe, m=m, export_returns_csv=False).ALL[0], (dataframe.ALL[m] / dataframe.ALL[0]) - 1)

    def test_get_returns(self):
        """
        Test if the dataframe created by get_returns is without missing values
        """
        dataframe_no_nan = get_returns(
            dataframe, export_returns_csv=False, m=1)
        bool_list = dataframe_no_nan.isnull().any()
        self.assertTrue(bool_list.any() == False)

    def test_binary_target(self):
        """
        Test of the binary_targets function. Sample randomly from the data frame and check the condition that led
        to the binary classification
        """
        dataframe_no_nan = get_returns(
            dataframe, export_returns_csv=False, m=1)
        binary_dataframe = binary_targets(
            dataframe_no_nan, export_binary_csv=False)

        for i in range(20):
            row = random.randint(0, binary_dataframe.shape[0])
            column = random.randint(0, binary_dataframe.shape[1] - 1)

            compare_list = list(dataframe_no_nan.iloc[row].values)
            compare_list.sort()
            compare_value = compare_list[int(len(compare_list) / 2)]

            random_column = binary_dataframe.iloc[:, column]
            random_value = random_column[row]

            self.assertTrue(random_value == 0 if random_value <=
                            compare_value else random_value == 1)


if __name__ == '__main__':
    unittest.main()
