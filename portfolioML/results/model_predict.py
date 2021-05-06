""" Several results of ANN models trained (LSTM, RAF, DNN, CNN) """
import argparse
import logging
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tensorflow as tf
from keras.models import load_model
from portfolioML.makedir import go_up, smart_makedir
from portfolioML.model.split import (all_data_DNN, all_data_LSTM,
                                     all_multidata_LSTM)
from sklearn.metrics import roc_auc_score, roc_curve


def plot_roc(algorithm, name_model, num_periods, wavelet):
    """
    Plot roc curve with mean and standard deviation of the area under the curve (auc) of a
    trained model.
    Each model is trained over several study period. Its .h5 file name contains this information
    that was put under this form: '<model_name>_periond<#>.h5' (for istance 'LSTM_Model3_period0.h5').
    Before running this function please carefully check the folder portfolioML/model/<algorithm>/<model_name>
    to avoid nuisance problems related to no file found.

    Tecnical aspects: because of each model has different tpr and fpr, an interpolation
    of this values is made in order to have the same lenght for each model.

    Parameters
    ----------
    model : string
        File path of the model, it must have in the name 'period' (for istance 'period0') to
        keep in mind the number of period over which the model is trained.

    num_periods : integer
        Number of period that are taken in order to compute plot and final values.
    wavelet : bool
        Set true if you have used DWT during the model training.

    Returns
    -------
    None
    """

    logging.info('----- I am creating ROC curve png files -----')

    tpr_list = []
    aucs_list = []
    interp_fpr = np.linspace(0, 1, 10000)

    smart_makedir(f'/ROC/{algorithm}/{name_model}/')

    path = os.getcwd() + f'/ROC/{algorithm}/{name_model}/'
    df_multiret_path = go_up(1) + "/data/MultidimReturnsData"

    df_returns = pd.read_csv(go_up(1) + "/data/ReturnsData.csv")
    df_binary = pd.read_csv(go_up(1) + "/data/ReturnsBinary.csv")
    df_multiret = [pd.read_csv(df_multiret_path + "1.csv"),
                   pd.read_csv(df_multiret_path + "2.csv"),
                   pd.read_csv(df_multiret_path + "3.csv"),
                   pd.read_csv(df_multiret_path + "4.csv")]

    plt.figure()
    for i in range(0, num_periods):
        logging.info(f'Creating ROC for period {i}')
        # Splitting data set for each period
        if ((algorithm == 'LSTM') or (algorithm == 'CNN')) and (wavelet == False):
            X_train, y_train, X_test, y_test = all_data_LSTM(
                df_returns, df_binary, i)
        if (algorithm == 'DNN'):
            X_train, y_train, X_test, y_test = all_data_DNN(
                df_returns, df_binary, i)
        if ((algorithm == 'LSTM') or (algorithm == 'CNN')) and (wavelet == True):
            X_train, y_train, X_test, y_test = all_multidata_LSTM(
                df_multiret, df_binary, i)

        model = load_model(
            go_up(1) + f'/model/{algorithm}/{name_model}/{name_model}_period{i}.h5')

        # ROC curve
        probas = model.predict(X_test)

        fpr, tpr, thresholds = roc_curve(y_test, probas[:, 0])

        interp_tpr = np.interp(interp_fpr, fpr, tpr)  # because of different len of tpr fpr
        tpr_list.append(interp_tpr)

        roc_auc = roc_auc_score(y_test, probas[:, 0], average=None)
        aucs_list.append(roc_auc)

        plt.plot(fpr, tpr, label=f'per{i} (area = %0.4f)' % (roc_auc))
        plt.plot([0, 1], [0, 1], 'k--')

    plt.xlabel('False Positive Rate',)
    plt.ylabel('True Positive Rate')
    plt.legend(loc="lower right", fontsize=12, frameon=False)
    plt.title(f'ROC CURVE {name_model}')
    plt.savefig(path + f'ROC CURVE {name_model}.png')

    auc_mean = np.mean(np.array(aucs_list))
    auc_std = np.std(np.array(aucs_list))

    tpr_mean = np.mean(tpr_list, axis=0)

    plt.figure()
    plt.plot(interp_fpr, tpr_mean, color='b',
             label=f'Mean ROC (AUC = {auc_mean:.4f} $\pm$ {auc_std:.4f})',
             lw=1, alpha=.8)

    tpr_std = np.std(tpr_list, axis=0)
    tprs_upper = np.minimum(tpr_mean + tpr_std, 1)
    tprs_lower = np.maximum(tpr_mean - tpr_std, 0)
    plt.fill_between(interp_fpr, tprs_lower, tprs_upper, color='blue', alpha=.2,
                     label=r'$\pm$ 1 std. dev.')
    plt.xlabel('False Positive Rate',)
    plt.ylabel('True Positive Rate')
    plt.legend(loc="lower right", fontsize=12, frameon=False)
    plt.title(f'ROC Curve {name_model} - mean +|- std')
    plt.savefig(path + f'ROC Curve {name_model} - mean +|- std.png')


def predictions_csv(algorithm, model_name, num_periods, wavelet):
    '''
    Create the csv files of forecasts.

    Parameters
    ----------
    algorithm : str
        Algorithm used (LSTM, DNN, RAF or CNN)
    model_name : str
        Name of the model (e.g. for LSTM check folder names in portfolio/model/LSTM/)
    num_periods : int
        Number of period that are taken in order to compute plot and final values. The default is 10.
    wavelet : bool
        Set true if you have used DWT during the model training.

    Returns
    -------
    None.

    '''

    logging.info('----- I am creating predictions csv file -----')

    smart_makedir(f'/predictions/{algorithm}/{model_name}/')
    df_multiret_path = go_up(1) + "/data/MultidimReturnsData"

    if wavelet:
        logging.info("==== PCA Reduction ====")
        df_multiret = [pd.read_csv(df_multiret_path + "1.csv"),
                       pd.read_csv(df_multiret_path + "2.csv"),
                       pd.read_csv(df_multiret_path + "3.csv"),
                       pd.read_csv(df_multiret_path + "4.csv")]
        df_binary = pd.read_csv(go_up(1) + "/data/ReturnsBinaryPCA.csv")
    else:
        df_returns = pd.read_csv(go_up(1) + "/data/ReturnsData.csv")
        df_binary = pd.read_csv(go_up(1) + "/data/ReturnsBinary.csv")

    for i in range(num_periods):
        model = load_model(
            go_up(1) + f'/model/{algorithm}/{model_name}/{model_name}_period{i}.h5')
        logging.info(f'Creating predictions csv file for period {i}')
        # Splitting data set for each period
        if ((algorithm == 'LSTM') or (algorithm == 'CNN')) and (wavelet == False):
            X_train, y_train, X_test, y_test = all_data_LSTM(
                df_returns, df_binary, i)
        if (algorithm == 'DNN'):
            X_train, y_train, X_test, y_test = all_data_DNN(
                df_returns, df_binary, i)
        if ((algorithm == 'LSTM') or (algorithm == 'CNN')) and (wavelet == True):
            X_train, y_train, X_test, y_test = all_multidata_LSTM(
                df_multiret, df_binary, i)

        y_pred = model.predict(X_test)
        y_pred_companies = [y_pred[i:87 + i]
                            for i in range(0, len(y_pred) - 87 + 1, 87)]
        dict_comp = {df_binary.columns[i]: y_pred_companies[i]
                     for i in range(len(df_binary.columns))}
        df_predictions = pd.DataFrame()
        for tick in df_binary.columns:
            df_predictions[tick] = dict_comp[tick][:, 0]
            df_predictions.to_csv(
                f'predictions/{algorithm}/{model_name}/{model_name}_Predictions_{i}th_Period.csv', index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Prediction and compare traned model')
    parser.add_argument('algorithm', type=str, help='CNN. LSTM or RAF')
    parser.add_argument('model_name', type=str,
                        help='Select the particular model trained')
    parser.add_argument('num_periods', type=int,
                        help="Number of period over which returns have to be calculated ")
    parser.add_argument('--pca_wavelet', '-p', action='store_true',
                        help='Set True if you have trained the model with DWT. Default: False')
    parser.add_argument("-log", "--log", default="info",
                        help=("Provide logging level. Example --log debug', default='info"))

    args = parser.parse_args()

    levels = {'critical': logging.CRITICAL,
              'error': logging.ERROR,
              'warning': logging.WARNING,
              'info': logging.INFO,
              'debug': logging.DEBUG}

    logging.basicConfig(level=levels[args.log])
    pd.options.mode.chained_assignment = None

    # Restrict memory allocation on GPU
    gpus = tf.config.experimental.list_physical_devices('GPU')
    if gpus:
        try:
            # Restrict TensorFlow to only use the fourth GPU
            tf.config.experimental.set_visible_devices(gpus[0], 'GPU')

            # Currently, memory growth needs to be the same across GPUs
            for gpu in gpus:
                tf.config.experimental.set_memory_growth(gpu, True)
            logical_gpus = tf.config.experimental.list_logical_devices('GPU')
            logging.info(len(gpus), "Physical GPUs,",
                         len(logical_gpus), "Logical GPUs")
        except RuntimeError as e:
            # Memory growth must be set before GPUs have been initialized
            logging.info(e)

    plot_roc(args.algorithm, args.model_name,
             args.num_periods, args.pca_wavelet)
    predictions_csv(args.algorithm, args.model_name,
                    args.num_periods, args.pca_wavelet)
