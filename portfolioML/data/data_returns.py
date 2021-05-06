"""Pre-processing dataframe"""
import argparse
import sys
import logging
from statistics import median

import numpy as np
import pandas as pd
from portfolioML.data.preprocessing import wavelet_dataframe


def get_returns(dataframe, export_returns_csv, m=1, no_missing=True):
    """
    Get day-by-day returns values for a company. The dataframe has companies as attributes
    and days as rows, the values are the close prices of each days.

    Parameters
    ----------
    dataframe : pandas.core.frame.DataFrame
        Input dataframe of prices.

    export_csv : bool
        Export dataframe in csv.

    m : int (optional)
        Period over which returns are calculated. The default is 1.

    no_missing : bool(optional)
        drop companies with missing values. The default is True.

    Returns
    -------
    df : pandas.core.frame.DataFrame
        Dataframe of m-period returns.
    """
    try:
        if m < 0:
            raise ValueError(
                "Ops! Invalid input, you can't go backward in time: m must be positive.")
    except ValueError as ve:
        print(ve)
        sys.exit()

    df = pd.DataFrame()
    for col in dataframe.columns[1:]:  # Not pick Data column
        today = dataframe[col]
        tomorrow = today[m:]
        df[col] = (np.array(tomorrow) / np.array(today)[:-m]) - 1

    if no_missing:
        df = df.dropna(axis=1)

    if export_returns_csv:
        df.to_csv('ReturnsData.csv', index=False)

    return df


def binary_targets(dataframe, export_binary_csv, name='ReturnsBinary'):
    """
    Returns binary value of returns for classification task, binary values are 0 and 1.
    To define the two classes, we order all m-period returns of all stocks 's'
    in period t + m in ascending order and cut them into two equally sized classes.
    Class 0 is realized if the m-period return of stock is smaller than the cross-sectional median return
    of all stocks in period t + 1 .
    Similarly, class 1 is realized if the m-period return of 's' is larger than or equal to the cross-sectional
    median.
    For more details see: https://doi.org/10.1016/j.ejor.2017.11.054

    Parameters
    ----------
    dataframe : pandas.core.frame.DataFrame
        Input dataframe of returns.

    export_binary_csv : bool
        Export dataframe in csv. The default is False.

    name : str(optional)
        Name of exported csv file.

    Returns
    -------
    df: pandas.core.frame.DataFrame
        Output dataframe of binary returns
    """
    df = dataframe
    for time_idx in range(dataframe.shape[0]):
        compare_list = list(dataframe.iloc[time_idx].values)
        compare_value = median(compare_list)

        df.iloc[time_idx] = dataframe.iloc[time_idx].apply(
            lambda x: 0 if x <= compare_value else 1)

    if export_binary_csv:
        df.to_csv(f'{name}.csv', index=False)
    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Process price data and get the dataframe of m period returns')
    parser.add_argument('-m', '--m_period_return', type=int,
                        default=1, help='m period return')
    parser.add_argument("--export_returns_csv", action='store_false',
                        help='Export to csv the dataframe of m-period price returns. The default is True')
    parser.add_argument("--export_binary_csv", action='store_false',
                        help='Export to csv the dataframe for the classification task. The default is True')
    parser.add_argument("-log", "--log", default="info",
                        help=("Provide logging level. Example --log debug', default='info"))

    args = parser.parse_args()

    levels = {'critical': logging.CRITICAL,
              'error': logging.ERROR,
              'warning': logging.WARNING,
              'info': logging.INFO,
              'debug': logging.DEBUG}

    logging.basicConfig(level=levels[args.log])

    df = pd.read_csv('PriceData.csv')
    df = df.drop(['Date'], axis=1)
    if df.columns[0] != 'Date':
        logging.info('Successfully removed Date column')

    dataframe_ritorni = get_returns(
        df, args.m_period_return, args.export_returns_csv)
    dataframe_binary = binary_targets(
        dataframe_ritorni, args.export_binary_csv)

    df_returns_pca = wavelet_dataframe('ReturnsData.csv', 'haar')
    df_binary_pca = binary_targets(
        df_returns_pca, args.export_binary_csv, name='ReturnsBinaryPCA',)
