""" Generate a csv file of daily close prices of all companies that are and were in the S&P500 index """
import argparse
import logging

import pandas as pd
import pandas_datareader as web
import requests
from bs4 import BeautifulSoup


def get_ticker():
    """
    Get tickers of companies in S&P500 over all time from Wikipedia page
    url = https://en.wikipedia.org/wiki/List_of_S%26P_500_companies#S&P_500_component_stocks

    Returns
    ------
    ticker : list
        list of tickers
    """
    website_url = requests.get(
        'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies#S&P_500_component_stocks').text
    soup = BeautifulSoup(website_url, 'lxml')

    idd_list = ['constituents', 'changes']
    df_list = list()
    for idd in idd_list:
        My_table = soup.find(
            'table', {'class': 'wikitable sortable', 'id': idd})
        df = pd.read_html(str(My_table))
        df = pd.DataFrame(df[0])
        df_list.append(df)

    df_list[1].columns = ['_'.join(col).strip()
                          for col in df_list[1].columns.values]
    df_list[1] = df_list[1].dropna()

    constituents = list(df_list[0].Symbol)
    added = list(df_list[1].Added_Ticker)
    removed = list(df_list[1].Removed_Ticker)

    ticker = list(set(constituents + added + removed))

    return ticker


def data_generator(start, end, data_source='yahoo', export_csv=True):
    '''
    Generate a pandas dataframe of historical close daily price.

    Parameters
    ----------
    start : str
        Start time. Its format must be yyyy-mm-dd

    end : str
        End time. Its format must be yyyy-mm-dd

    data_source : str(optional)
        The data source ("iex", "fred", "ff"). Default = 'yahoo'

    export_csv : bool(optional)
        Choose whether to export to csv. Default = True


    Returns
    -------
    data : pandas.core.frame.DataFrame
        Pandas dataframe of historical close daily price
    '''

    tickers = get_ticker()
    data_list = {}
    for ticks in tickers:
        try:
            data_list[ticks] = web.DataReader(
                ticks, data_source=data_source, start=start, end=end).Close
            logging.info(f'Downloading data of {ticks}')
        except Exception:
            logging.info(f"There's no data for {ticks}")
    data = pd.DataFrame(data_list)

    if export_csv:
        data.to_csv('PriceData.csv')

    return data


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Generator of historical price data')
    parser.add_argument('-start', type=str,
                        default="1995-01-01", help="Start time")
    parser.add_argument(
        '-end', type=str, default="2020-12-31", help="End time")
    parser.add_argument("-l", "--log", default="info",
                        help=("Provide logging level. Example --log debug', default='info"))
    args = parser.parse_args()
    levels = {'critical': logging.CRITICAL,
              'error': logging.ERROR,
              'warning': logging.WARNING,
              'info': logging.INFO,
              'debug': logging.DEBUG}

    logging.basicConfig(level=levels[args.log])
    data = data_generator(args.start, args.end)
