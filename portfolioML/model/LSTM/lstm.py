"""LSTM model"""
import argparse
import logging
import os

import matplotlib.pyplot as plt
import pandas as pd
from keras.callbacks import EarlyStopping, ModelCheckpoint
from keras.layers import LSTM, Dense, Dropout, Input
from keras.models import Sequential, load_model
from keras.optimizers import Adam, RMSprop
from keras.utils.vis_utils import plot_model
from portfolioML.makedir import go_up, smart_makedir
from portfolioML.model.split import all_data_LSTM, all_multidata_LSTM


def LSTM_model(nodes, optimizer, dim, drop_out=0.2, plot_figure=True):
    '''
    Architeture for the LSTM algorithm.

    Parameters
    ----------
    nodes : list
        The lenght of this list is equal to the number of LSTM+Dropout layers.
        The elements correspond to the numbero of nodes of each LSTM layer.
    optimizer : str
        Optimizier between RMS_prop or Adam.
    dim : int
        Number of features.
    drop_out : float, optional
        Value of the dropout in all the dropout layers. The default is 0.2.
    plot_figure : bool
        Choose whether or not to plot the architeture of the model.

    Returns
    -------
    model : tensorflow.python.keras.engine.sequential.Sequential
        Model.

    '''

    model = Sequential()
    model.add(Input(shape=(240, dim)))
    model.add(Dropout(drop_out))

    if len(nodes) > 1:
        ret_seq = True
    else:
        ret_seq = False

    for nod in nodes:
        model.add(LSTM(nod, return_sequences=ret_seq))
        model.add(Dropout(drop_out))

    model.add(Dense(1, activation='sigmoid'))

    # Two optimiziers used during training
    if optimizer == 'RMS_prop':
        opt = RMSprop(learning_rate=0.005, momentum=0.5, clipvalue=0.5)
    if optimizer == 'Adam':
        opt = Adam(learning_rate=0.005)

    model.compile(loss='binary_crossentropy',
                  optimizer=opt, metrics=['accuracy'])

    if plot_figure:
        plot_model(model, to_file=f'LSTM:{nodes}_{optimizer}_{drop_out}.png',
                   show_shapes=True, show_layer_names=True)
    return model


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Creation of input and output data for lstm classification problem')
    parser.add_argument("-log", "--log", default="info",
                        help=("Provide logging level. Example --log debug', default='info"))
    parser.add_argument('num_periods', type=int,
                        help='Number of periods you want to train')
    parser.add_argument('nodes', type=int, nargs='+',
                        help='Choose the number of nodes in LSTM+Dropout layers')
    parser.add_argument('model_name', type=str,
                        help='Choose the name of the model')
    parser.add_argument('-p', '--pca_wavelet', action='store_true',
                        help='Use the most important companies obtained by a PCA decomposition on the first 250 PCs and then DWT. Default: False')
    parser.add_argument('-r', '--recursive', action='store_false',
                        help='Choose whether or not to pass parameters from one period to another during training. Default: True')
    parser.add_argument('-o', '--optimizer', type=str, default='RMS_prop',
                        help='Choose RMS_prop or Adam. Default: RMS_prop')

    args = parser.parse_args()

    levels = {'critical': logging.CRITICAL,
              'error': logging.ERROR,
              'warning': logging.WARNING,
              'info': logging.INFO,
              'debug': logging.DEBUG}

    logging.basicConfig(level=levels[args.log])
    pd.options.mode.chained_assignment = None  # Mute some warnings of Pandas
    # Get data paths
    df_multidimret_path = go_up(2) + "/data/MultidimReturnsData"

    # Compute PCA reduction
    if args.pca_wavelet:
        logging.info("==== PCA Reduction ====")
        df_multiret = [pd.read_csv(df_multidimret_path + "1.csv"),
                       pd.read_csv(df_multidimret_path + "2.csv"),
                       pd.read_csv(df_multidimret_path + "3.csv"),
                       pd.read_csv(df_multidimret_path + "4.csv")]
        df_binary = pd.read_csv(go_up(2) + "/data/ReturnsBinaryPCA.csv")
    else:
        df_returns = pd.read_csv(go_up(2) + "/data/ReturnsData.csv")
        df_binary = pd.read_csv(go_up(2) + "/data/ReturnsBinary.csv")

    smart_makedir(args.model_name)
    smart_makedir(args.model_name + "/accuracies_losses")

    for i in range(args.num_periods):
        logging.info(f'============ Start Period {i}th ===========')

        # Compute DWT decomposition
        if args.pca_wavelet:
            logging.info("==== DWT ====")
            X_train, y_train, X_test, y_test = all_multidata_LSTM(
                df_multiret, df_binary, i)
        else:
            X_train, y_train, X_test, y_test = all_data_LSTM(
                df_returns, df_binary, i)

        if (i != 0) and (args.recursive):
            logging.info('LOADING PREVIOUS MODEL')
            model = load_model(
                f"{args.model_name}/{args.model_name}_period{i-1}.h5")
        else:
            logging.info('CREATING NEW MODEL')
            model = LSTM_model(args.nodes, args.optimizer, X_train.shape[2])
        logging.info(model.summary())

        es = EarlyStopping(monitor='val_loss', patience=30,
                           restore_best_weights=True)
        mc = ModelCheckpoint(f'{args.model_name}/{args.model_name}_period{i}.h5',
                             monitor='val_loss', mode='min', verbose=0)
        history = model.fit(X_train, y_train, epochs=800, batch_size=768,
                            callbacks=[es, mc], validation_split=0.2, shuffle=False, verbose=1)

        plt.figure(f'Loss and Accuracy Period {i}', figsize=[20.0, 10.0])
        plt.subplot(1, 2, 1)
        plt.plot(history.history['loss'], label='train_loss')
        plt.plot(history.history['val_loss'], label='val_loss')
        plt.xlabel('Epochs')
        plt.title('Training and Validation Losses vs Epochs')
        plt.grid()
        plt.legend()

        plt.subplot(1, 2, 2)
        plt.plot(history.history['accuracy'], label='accuracy')
        plt.plot(history.history['val_accuracy'], label='val_accuracy')
        plt.xlabel('Epochs')
        plt.title('Training and Validation Accuracies vs Epochs')
        plt.grid()
        plt.legend()
        plt.savefig(
            os.getcwd() + f'/{args.model_name}/accuracies_losses/accuracies_{i}.png')

        with open(f"{args.model_name}/{args.model_name}_specifics.txt", 'w', encoding='utf-8') as file:
            file.write(
                f'''\n Model Name: {args.model_name}
                  \n Number of periods: {args.num_periods}
                  \n Number of nodes: {args.nodes}
                  \n Optimizier: {args.optimizer}
                  \n PCA + Wavelet: {args.pca_wavelet} \n''')
            model.summary(print_fn=lambda x: file.write(x + '\n'))

        logging.info(f'============ End Period {i}th ===========')
