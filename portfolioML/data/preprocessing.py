''' Pre process data in order to capture its most important aspects. The methods
implemented are DWT and PCA '''
import logging

import numpy as np
import pandas as pd
import pywt
from sklearn.decomposition import PCA


def approx_details_scale(data, wavelet, dec_level):
    """
    Approximation and details signal of a time series at specific time scale.

    Parameters
    ----------
    data: list, numpy array
        Input time-series data.

    wavelet: string
        Wavelet's name used for the decomposition.
        For all available wavelet see https://pywavelets.readthedocs.io/en/latest/ref/wavelets.html.

    dec_level: integer
        Level of time_scale on which compute the approximations and details.
        Its range is [1, pywt.dwtn_max_level(data, wavelet)+1].

    Returns
    ------
    approx: numpy array
        Approximation values.

    detail: numpy array
        Details values.
    """

    max_level = pywt.dwt_max_level(len(data), wavelet)

    try:
        if dec_level > max_level + 1:
            raise ValueError(
                'Decomposition level choosen is bigger that the maximal allowed one')
    except Exception as ex:
        logging.error(ex)
        dec_level = max_level + 1

    coeffs = pywt.wavedec(data, wavelet, level=dec_level)

    for i in range(2, len(coeffs)):
        coeffs[i] = np.zeros_like(coeffs[i])

    det = coeffs[1]

    coeffs[1] = np.zeros_like(coeffs[1])
    approx = pywt.waverec(coeffs, wavelet)

    coeffs[1] = det
    coeffs[0] = np.zeros_like(coeffs[0])
    details = pywt.waverec(coeffs, wavelet)

    return approx, details


def pca(df_returns_path, n_components=250):
    '''
    Compute the PCA decomposition of a dataset.

    Parameters
    ----------
    df_returns_price : str
        Path to the csv dataframe file.
    n_components : int
        Number of components you want your dataframe be projected to.

    Returns
    -------
    most_important_features : list
        List of the most important features in the dataset.

    '''

    df_returns = pd.read_csv(df_returns_path)
    pca = PCA(n_components=n_components)
    pca.fit(df_returns.values)
    logging.info(
        f"Fraction of variance preserved: {pca.explained_variance_ratio_.sum():.2f}")

    n_pca = pca.n_components_  # get number of components
    # Get the index of the most important feature on each component
    most_important = [np.abs(pca.components_[i]).argmax()
                      for i in range(n_pca)]
    initial_feature_names = df_returns.columns
    # Get the most important feature names
    most_important_features = [initial_feature_names[most_important[i]]
                               for i in range(n_pca)]
    most_important_features = list(set(most_important_features))
    df_returns[most_important_features].to_csv(
        'ReturnsDataPCA.csv', index=False)

    return most_important_features


def wavelet_dataframe(df_returns_path, wavelet):
    '''
    Compute the DWT (Discrete Wavelet Tranform) of a dataset composed by multiple
    time signals reduced by PCA. The action of this function is to export in csv
    two dataframes: one composed by daily close prices of the most important companies
    that PCA computed and one composed by the first three details coefficients and
    the fourth approximation computed by DWT for each of the most important companies.

    Two words on DWT:
    "Discrete  Wavelet  Transform  (DWT)  can decompose
    the  signal  in  both  timeand  frequency  domain  simultaneously.On  the  other
    hand,Fourier  Transform  decomposesthe signal  only  in  frequency  domain;
    information  related  to occurrence  of  frequency  is  not  captured  and  it
    eliminates  the  time  resolution"  (Ortega  & Khashanah,2014).

    Parameters
    ----------
    df_price_path : str
        Path of the pandas dataframe of time signals.
    wavelet : str
        Which wavelet is used during the decomposition.

    Returns
    -------
        None
    '''

    df_returns1 = pd.read_csv('ReturnsData.csv')

    most_imp_comp = pca(df_returns_path)
    logging.info(f"Number of companies choosen by PCA: {len(most_imp_comp)}")
    df_returns1 = df_returns1[most_imp_comp]

    dic1, dic2, dic3, dic4 = {}, {}, {}, {}
    for tick in df_returns1.columns:
        a1, d1 = approx_details_scale(df_returns1[tick], wavelet, 1)
        a2, d2 = approx_details_scale(df_returns1[tick], wavelet, 2)
        a3, d3 = approx_details_scale(df_returns1[tick], wavelet, 3)
        dic1[tick], dic2[tick], dic3[tick], dic4[tick] = d1, d2, d3, a3
    dataframe1 = pd.DataFrame(dic1)
    dataframe2 = pd.DataFrame(dic2)
    dataframe3 = pd.DataFrame(dic3)
    dataframe4 = pd.DataFrame(dic4)
    dataframe1.to_csv("MultidimReturnsData1.csv", index=False)
    dataframe2.to_csv("MultidimReturnsData2.csv", index=False)
    dataframe3.to_csv("MultidimReturnsData3.csv", index=False)
    dataframe4.to_csv("MultidimReturnsData4.csv", index=False)
