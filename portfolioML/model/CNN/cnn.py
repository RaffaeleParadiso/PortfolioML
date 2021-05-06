""" CNN model """
import argparse
import logging
import os

import matplotlib.pyplot as plt
import pandas as pd
from keras.callbacks import EarlyStopping, ModelCheckpoint
from keras.layers import (Concatenate, Conv1D, Dense, Dropout, Flatten, Input,
                          MaxPooling1D)
from keras.models import Model
from keras.utils.vis_utils import plot_model
from portfolioML.makedir import go_up, smart_makedir
from portfolioML.model.split import all_data_LSTM, all_multidata_LSTM


def CNN_model(filters, dim, kernel_size=(20), strides=5, activation='tanh', min_pooling=False, plt_figure=False):
    """
    CNN model with selected number of filters for the convolutional layers for classification
    task applied to time-series of data. One convolutional layes is enough fot his problem.

    The basic structure is:

    - Inputs: shape=(240,1). The shape reflects the number of close values for each instance (240)
    and the number of features (1)

    - Dropout: drop(0.1)

    - Convolution: Conv1D(filters, kernel_size=kernel_size, strides=strides, activation='tanh')(drop)
    The choice of parameters follows the "nature" of the task, the kernel_size is almost one month
    of trading days (20) and the stride is one week (5).

    - Pooling Layers:
            -- MaxPooling: MaxPooling1D(pool_size=5, strides=1, padding='valid')(conv)
            find the max value in a pool_size of 5.

            -- If min_pooling = True
                - MinPooling: MinPooling1D(pool_size=5, strides=1, padding='valid').pooling(conv)
                Find the min value in a pool_size of 5.

                - Merge: Concatenate()([min_pool, max_pool])
                Merge two pooling layers that have the same input to create a multiheaded structure.

    - Dropout: drop(0.1)

    - Dense: Dense(25, activation= 'tanh')(flatten)
    Compute all the features and the information from the previous steps.

    - Output: Dense(1, activation='sigmoid'), the output is interpretated as the probability that
    the input is grater than the cross-sectional median.

    Parameters
    ----------
    filters: integer
        The dimensionality of the output space (the number of output filters in the convolution).
        Reference: https://keras.io/api/layers/convolution_layers/convolution1d/

    kernel_size: tuple of one integers (optional)
        Specifying the length of the 1D convolution window. Default = (20)
        Reference: https://keras.io/api/layers/convolution_layers/convolution1d/

    strides: integer
        Specify the stride length of the convolution. Default = 5
        Specifying any stride value != 1 is incompatible with specifying any dilation_rate value != 1.
        Reference: https://keras.io/api/layers/convolution_layers/convolution1d/

    activation: function to use (optional)
        If you don't specify anything, no activation is applied (see keras.activations).
        Default = 'tanh'
        Reference: https://keras.io/api/layers/convolution_layers/convolution1d/

    plot_figure : bool
        Choose whether or not to plot the architeture of the model


    Results
    -------
    model: tensorflow.python.keras.engine.sequential.Sequential
        tensorflow model with selected hidden layers
    """
    inputs = Input(shape=(240, dim))
    drop = Dropout(0.1)(inputs)

    conv = Conv1D(filters, kernel_size=kernel_size,
                  strides=strides, activation=activation)(drop)
    max_pool = MaxPooling1D(pool_size=5, strides=1)(conv)
    if min_pooling:
        min_pool = -MaxPooling1D(pool_size=5, strides=1)(-conv)  # MinPooling
        merge = Concatenate()([min_pool, max_pool])
        drop = Dropout(0.4)(merge)
    else:
        drop = Dropout(0.4)(max_pool)

    flatten = Flatten()(drop)

    dense = Dense(25, activation='tanh')(flatten)
    outputs = Dense(1, activation='sigmoid')(dense)

    model = Model(inputs=inputs, outputs=outputs)
    model.compile(loss='binary_crossentropy',
                  optimizer='adam', metrics=['accuracy'])

    if plt_figure:
        plot_model(model, to_file=f'CNN:fil{filters}_kernel{kernel_size}_strides{strides}_min{min_pooling}.png',
                   show_shapes=True, show_layer_names=True)

    logging.info(model.summary())
    return model


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Make CNN model for classification task to predict class label 0 or 1')
    parser.add_argument("filters", type=int,
                        help="filters of convolutional layers")
    parser.add_argument('model_name', type=str,
                        help='Choose the name of the model')
    parser.add_argument('num_periods', type=int,
                        help='Number of periods you want to train')
    parser.add_argument("-kernel_size", type=int, default=20,
                        help="kernel_size, for more details see documentation of CNN_model")
    parser.add_argument("-strides", type=int, default=5,
                        help="strides, for more details see documentation of CNN_model")
    parser.add_argument("-activation", type=str, default='tanh',
                        help="activation, for more details see documentation of CNN_model")
    parser.add_argument("-min_pooling", action='store_true',
                        help="If true the structure is multiheaded")
    parser.add_argument("-plt_figure", action='store_true',
                        help="If true create png file of the model")
    parser.add_argument('-p', '--pca_wavelet', action='store_true',
                        help='Use the most important companies obtained by a PCA decomposition on the first 250 PCs and then DWT. Default: False')

    parser.add_argument("-log", "--log", default="info",
                        help=("Provide logging level. Example --log debug', default='info"))

    args = parser.parse_args()

    levels = {'critical': logging.CRITICAL,
              'error': logging.ERROR,
              'warning': logging.WARNING,
              'info': logging.INFO,
              'debug': logging.DEBUG}

    logging.basicConfig(level=levels[args.log])

    # Get data paths
    df_multidimret_path = go_up(2) + "/data/MultidimReturnsData"

    # Read the data
    if args.pca_wavelet:
        logging.info("==== PCA Reduction and Wavelet Decomposition ====")
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

    for per in range(args.num_periods):
        logging.info(f'============ Start Period {per}th ===========')

        # Compute DWT decomposition
        if args.pca_wavelet:
            logging.info("==== DWT ====")
            X_train, y_train, X_test, y_test = all_multidata_LSTM(
                df_multiret, df_binary, per)
        else:
            X_train, y_train, X_test, y_test = all_data_LSTM(
                df_returns, df_binary, per)

        model = CNN_model(args.filters, dim=X_train.shape[2], kernel_size=tuple((args.kernel_size,)),
                          strides=args.strides, activation=args.activation,
                          min_pooling=args.min_pooling, plt_figure=args.plt_figure)

        es = EarlyStopping(monitor='val_loss', patience=40,
                           restore_best_weights=True)
        mc = ModelCheckpoint(f'{args.model_name}/{args.model_name}_period{per}.h5',
                             monitor='val_loss', mode='min', verbose=0)
        history = model.fit(X_train, y_train, callbacks=[
                            es, mc], validation_split=0.2, batch_size=512, epochs=400, verbose=1)

        # Elbow curve
        plt.figure(f'{args.model_name} and Accuracy Period {per}',
                   figsize=[20.0, 10.0])
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
            os.getcwd() + f'/{args.model_name}/accuracies_losses/accuracies_{per}.png')

    plt.show()

    with open(f"{args.model_name}/{args.model_name}_specifics.txt", 'w', encoding='utf-8') as file:
        file.write(
            f'\n Model Name: {args.model_name} \n Number of periods: {args.num_periods} \n Number of filters in conv layers: {args.filters} \n PCA + Wavelet: {args.pca_wavelet} \n')
        model.summary(print_fn=lambda x: file.write(x + '\n'))
