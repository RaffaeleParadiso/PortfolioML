'''Random Forest Classifier'''
import argparse
import logging
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from portfolioML.makedir import go_up, smart_makedir
from portfolioML.model.split import all_data_DNN
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score, roc_curve, classification_report, confusion_matrix, accuracy_score
from sklearn.model_selection import train_test_split

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")


def predictions_and_roc(df_returns, df_binary, n_estimators, max_depth, num_period, criterion, autoencoder=False):
    '''
    Create the csv files of forecasts and plot the roc curve
    with mean and standard deviation of the area under the curve (auc).

        To classify the binary target, the random forest classifier fits a large number
        of decision tree on varius samples of the dataset and then make the classification.
        The variable n_estimators is the number of decision trees in the random forest.
        Every decision tree has a choosen maximum depth controlled by the variable max_depth.
        The function to measure the quality of a split is given by the variable criterion.
        The deafult criterion is the “gini” index for the measure of Gini impurity at each split
        Note: this parameter is tree-specific.

        For more information, visit:
        https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestClassifier.html

    Parameters
    ----------
    n_estimators : integer
        Number of threes in the random forest.

    max_depth : integer
        Depth of trees in the random forest.

    num_period : integer
        Number of period that are taken in order to compute plot and final values.

    criterion : str
        Criterion for splitting the nodes.
    ----------

    Each model is trained over several study period so its name contain this information,
    So before running this function, carefully checking the existence of the folder,
    in order to avoid problems and lost the previous forecast.

    Tecnical aspects: because of each model has got different tpr and fps, an interpoletion
    of this values is used in order to have the same leght for each model.
    '''
    if autoencoder:
        df_auto_train_path = go_up(2) + "/data/after_train.csv"
        df_auto_test_path = go_up(2) + "/data/after_test.csv"
        auto_train = pd.read_csv(df_auto_train_path)
        auto_test = pd.read_csv(df_auto_test_path)
        x_train_auto = np.array(auto_train)
        x_test_auto = np.array(auto_test)

    path_n = go_up(level_up=2)
    path_p = path_n + f'/results/predictions/RAF/RAF_{args.name_model}/'
    path_r = path_n + f'/results/ROC/RAF/RAF_{args.name_model}/'

    tpr_list = []
    aucs_list = []
    interp_fpr = np.linspace(0, 1, 10000)
    plt.figure()

    accuracy = []
    for i in range(0, num_period):
        logging.info('============ Start Period %ith ===========', i)

        x_train, y_train, x_test, y_test = all_data_DNN(
            df_returns, df_binary, i)
        rf_ = RandomForestClassifier(n_estimators, criterion, max_depth, n_jobs=-1,
                                     min_samples_split=2, min_samples_leaf=1,
                                     verbose=1)
        if autoencoder_features:
            x_train = x_train_auto
            x_test = x_test_auto
        # creation train and validation set
        X_train, X_val, Y_train, Y_val = train_test_split(x_train, y_train, test_size=0.2, shuffle=False)
        rf_.fit(X_train, Y_train)

        y_proba = rf_.predict(X_val) # prediction on validation set
        accuracy.append(accuracy_score(Y_val, y_proba)) # accuracies on validation set 
        y_proba = rf_.predict(x_test) #prediction on test set

        fpr, tpr, thresholds = roc_curve(y_test, y_proba)
        interp_tpr = np.interp(interp_fpr, fpr, tpr)
        tpr_list.append(interp_tpr)
        roc_auc = roc_auc_score(y_test, y_proba)
        aucs_list.append(roc_auc)

        plt.plot(fpr, tpr, label=f'per{i} (area = %0.4f)' % (roc_auc))
        plt.plot([0, 1], [0, 1], 'k--')
        plt.xlabel('False Positive Rate',)
        plt.ylabel('True Positive Rate')
        plt.legend(loc="lower right", fontsize=12, frameon=False)
        plt.title('ROC CURVE')
        plt.savefig(path_r + 'ROC_CURVE.png')

        y_pred_companies = [y_proba[i:87 + i]
                            for i in range(0, len(y_proba) - 87 + 1, 87)]
        dict_comp = {df_returns.columns[i]:
                     y_pred_companies[i] for i in range(len(df_returns.columns))}
        df_predictions = pd.DataFrame()
        for tick in df_returns.columns:
            df_predictions[tick] = dict_comp[tick]
            df_predictions.to_csv(path_p + f'RAF_{args.name_model}_Predictions_{i}th_Period.csv',
                                  index=False)

        logging.info('============= End Period %ith ============', i)

    acc = np.array(accuracy)
    auc_mean = np.mean(np.array(aucs_list))
    auc_std = np.std(np.array(aucs_list))
    tpr_mean = np.mean(tpr_list, axis=0)

    plt.figure()
    plt.bar(np.arange(1, args.num_period + 1), acc)
    plt.xticks(np.arange(1,args.num_period + 1))
    plt.title('Accuracy vs Periods')
    plt.xlabel('Periods')
    plt.ylabel('Accuracies')
    plt.savefig(f'accuracy_for_RAF_{args.name_model}')

    mean_acc = np.mean(accuracy)
    logging.info(f'Mean accuracy: {mean_acc} over {args.num_period} periods')

    plt.figure()
    plt.plot(interp_fpr, tpr_mean, color='b',
             label=fr'Mean ROC (AUC = {auc_mean:.4f} $\pm$ {auc_std:.4f})', lw=1, alpha=.8)
    tpr_std = np.std(tpr_list, axis=0)
    tprs_upper = np.minimum(tpr_mean + tpr_std, 1)
    tprs_lower = np.maximum(tpr_mean - tpr_std, 0)
    plt.fill_between(interp_fpr, tprs_lower, tprs_upper, color='blue', alpha=.2,
                     label=r'$\pm$ 1 std. dev.')
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.legend(loc="lower right", fontsize=12, frameon=False)
    plt.title('ROC Curve - mean +|- std')
    plt.savefig(path_r + 'ROC Curve - mean +|- std')

    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='RandomForestClassifier')
    parser.add_argument("-log", "--log", default="info",
                        help=("Provide logging level. Example --log debug', default='info"))
    requiredNamed = parser.add_argument_group('Required named arguments')
    requiredNamed.add_argument('-n', '--num_period', type=int, default=17,
                               help='Number of periods you want to train (leave blanck for default=17)')
    requiredNamed.add_argument('-ne', '--n_estimators', type=int, default=1000,
                               help='Number of trees (leave blanck for  default=1000)')
    requiredNamed.add_argument('-md', '--max_depth', type=int, default=25,
                               help='Trees\'s depth (leave blanck for  default=25) ')
    requiredNamed.add_argument('name_model', type=str, help='Name_model')
    parser.add_argument('-ae', '--autoencoder', action="store_true",
                        help='Features selected from autoencoder? (default=False)')
    parser.add_argument('-c', '--criterion', type=str, default='gini',
                        help='Criterion (default=gini)')
    args = parser.parse_args()

    levels = {'critical': logging.CRITICAL,
              'error': logging.ERROR,
              'warning': logging.WARNING,
              'info': logging.INFO,
              'debug': logging.DEBUG}

    logging.basicConfig(level=levels[args.log])
    pd.options.mode.chained_assignment = None

    autoencoder_features = args.autoencoder

    if autoencoder_features:
        df_returns_path = go_up(2) + "/data/ReturnsDataPCA.csv"
        df_binary_path = go_up(2) + "/data/ReturnsBinaryPCA.csv"
        df_returns = pd.read_csv(df_returns_path)
        df_binary = pd.read_csv(df_binary_path)

    else:
        df_returns_path = go_up(2) + "/data/ReturnsData.csv"
        df_binary_path = go_up(2) + "/data/ReturnsBinary.csv"
        df_returns = pd.read_csv(df_returns_path)
        df_binary = pd.read_csv(df_binary_path)

    smart_makedir(
        f'/results/predictions/RAF/RAF_{args.name_model}/', level_up=2)
    smart_makedir(f'/results/ROC/RAF/RAF_{args.name_model}/', level_up=2)

    predictions_and_roc(df_returns, df_binary, n_estimators=args.n_estimators, max_depth=args.max_depth,
                        num_period=args.num_period, criterion=args.criterion, autoencoder=autoencoder_features)

    with open(f"RAF_{args.name_model}.txt", 'a', encoding='utf-8') as file:
        file.write(
            f'''\n Number of periods: {args.num_period}
                \n Number of estimators: {args.n_estimators}
                \n Criterion: {args.criterion}
                \n Maximum depth of trees: {args.max_depth}
                \n Autoencoder: {args.autoencoder}''')
