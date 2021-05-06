"""Unit test portfolio_creation.py"""
import os
import random
import unittest

import pandas as pd
from portfolioML.makedir import go_up


def _full_path(file_name):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), file_name)


class TestDataReturns(unittest.TestCase):

    def test_get_trading_values(self):
        """Test of get_trading_values fuction, the first columns
            must be of type str and named as Date"""
        rand = random.randint(0, 9)
        rand1 = random.randint(0, 3)
        algorithm = ['CNN', 'RAF', 'LSTM', 'DNN']
        model_name = ['CNN_dense', 'RAF_Model1', 'LSTM_Model3', 'DNN_paper']
        path = f'''/results/predictions_for_portfolio/
                        {algorithm[rand1]}/{model_name[rand1]}/Trading_days_period{rand}.csv'''
        trading_days = pd.read_csv(_full_path(go_up(1) + f'/PortfolioML/portfolioML/results/predictions_for_portfolio/{algorithm[rand1]}/{model_name[rand1]}/Trading_days_period{rand}.csv'))
        first_column = trading_days.columns[0]
        self.assertEqual(first_column, 'Date')
        self.assertIsInstance(trading_days[first_column][0], str)


if __name__ == '__main__':
    unittest.main()
