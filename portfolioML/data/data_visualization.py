"""LSTM model"""
import argparse
import logging
import os
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from portfolioML.data.preprocessing import approx_details_scale
from portfolioML.makedir import go_up

# sys.path.append(os.path.dirname(os.path.abspath("..")))


def stock_features_df(dataframe, time_feature_steps=5):
    """
    DEPRECATED.
    Given a dataset of returns(prices) it returns the feature at each time_feature_steps.
    the featare are now only the mean values of returns(prices) each time_feature_steps

    Parameters
    ----------
    dataframe: pandas dataframe
        Input dtaframe

    time_feature_steps: integer (optional)
        Time window over which compute the feature. Default = 10

    Returns
    -------
    data_feature: pandas dataframe
        Output dataframe. It has gat the companies as istances(records) and the feature
        as attributes
    """

    df_feature = [dataframe[i:i + time_feature_steps].mean()
                  for i in range(0, len(dataframe), time_feature_steps)]
    data_feature = pd.DataFrame(df_feature[:-1])
    data_feature = data_feature.transpose()

    new_attributes = {i: f'{i+1}-Period' for i in data_feature.columns}
    data_feature = data_feature.rename(columns=new_attributes)

    return data_feature


def plot_wavelet(df_price, data, name, time_scale=3):
    """
    Plot original data and wavelet decomposition of input data.
    DWT is compute over selected time scale.

    Parameters
    ----------
    df_price : pandas.core.frame.DataFrame
        Dataframe of price data.

    data : numpy array
        Array of input time-series data

    name : string
        Figure name

    time_scale : integer(optional)
        Time scales over wich compute the DWT. Default = 3
    """
    df_price = pd.read_csv('PriceData.csv')

    days = df_price['Date']
    x_label_position = np.arange(0, len(days), 150)
    x_label_day = [days[i] for i in x_label_position]

    plt.figure(figsize=[15, 15])
    plt.subplot(time_scale + 2, 1, 1)  # num of subplots = time_scale plots + approximation + original data
    plt.plot(data, lw=0.9, c='mediumblue', label='original time-series')
    plt.title("Discrete_Wavelet_Trasformation_of_Close_" + name + "_Data")
    plt.xticks([])
    plt.legend()

    for scale in range(1, time_scale + 1):
        app, det = approx_details_scale(data, 'haar', scale)

        if scale == time_scale:
            plt.subplot(time_scale + 2, 1, scale + 1)
            plt.plot(det, lw=0.9, c='cornflowerblue',
                     label=f"details coefficients on scale {scale}")
            plt.xticks([])
            plt.legend()

            plt.subplot(time_scale + 2, 1, scale + 2)
            plt.plot(app, lw=0.9, c='cornflowerblue',
                     label=f"approximation on scale {scale}")
            plt.xticks(x_label_position, x_label_day, rotation=60)
            plt.legend()
        else:
            plt.subplot(time_scale + 2, 1, scale + 1)
            plt.plot(det, lw=0.9, c='cornflowerblue',
                     label=f"details coefficients on scale {scale}")
            plt.xticks([])
            plt.legend()

    plt.savefig("Discrete_Wavelet_Trasformation_of_Close_" + name + "_Data")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Creation of input and output data for lstm classification problem')
    parser.add_argument("-time_feature_steps", type=int, default=240,
                        help="Window over which you compute the mean")
    parser.add_argument("-log", "--log", default="info",
                        help=("Provide logging level. Example --log debug', default='info"))
    args = parser.parse_args()

    levels = {'critical': logging.CRITICAL,
              'error': logging.ERROR,
              'warning': logging.WARNING,
              'info': logging.INFO,
              'debug': logging.DEBUG}

    logging.basicConfig(level=levels[args.log])

    # Read the data
    df_price = pd.read_csv('PriceData.csv')
    df_returns = pd.read_csv('ReturnsData.csv')
    df_price = df_price.dropna(axis=1)

    # Data Visualization
    days = df_price['Date']
    x_label_position = np.arange(0, len(days), 150)
    x_label_day = [days[i] for i in x_label_position]

    plt.figure(figsize=[15, 15])
    for i in np.random.randint(1, 365, 5):
        comp = df_price.columns[i]
        plt.plot(df_price[comp], lw=0.8, label=f"{comp}")
    plt.xticks(x_label_position, x_label_day, rotation=60)
    plt.legend()
    plt.grid()
    plt.title("Close price of several Companies")
    plt.ylabel("Close price")
    plt.xlabel("Days")
    plt.savefig("Five_price_data.png")

    df_price = pd.read_csv('PriceData.csv')
    df_returns = pd.read_csv('ReturnsData.csv')

    # Wavelet decomposition of a single company
    Price = np.array(df_price['DIS'])
    Return = np.array(df_returns['DIS'])

    plot_wavelet(df_price, Price, "Price", time_scale=3)
    plot_wavelet(df_price, Return, "Return", time_scale=3)

    plt.show()
