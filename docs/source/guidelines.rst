.. _guidelines:

Guidelines
=============

Initial Setup
-------------
To run this project, clone the repository

.. code-block:: bash

   $ git clone https://github.com/DanieleMDiNosse/PortfolioML.git

If you use conda, you can create a new conda environment with:

.. code-block:: bash

   $ conda create -n <name_environment>
   $ conda activate <name_environment>

And then install with:

.. code-block:: bash

   $ cd ../PortfolioML/
   $ pip install .

How to use the package
----------------------
.. note::
   For every module, run at first

   .. code-block:: python

      python <module_name> -h

   to check the available options.


There's a logic progression that should be used in order to obtain our results:

1. Run ``portfolioML.data.data_generator`` to obtain the csv file of price data.
2. Run ``portfolioML.data.data_returns``. It will generate all the csv files needed for the study.
3. At this point you have all you need to train one of the ML algorithms implemented.
   Go to one of their folder and run, for example, ``portfolioML.model.LSTM.lstm``.
   It will save all the trained models in .h5 format in a new folder named as you choosen
   and some plot of losses and accuracies in another one called 'accuracies_losses'.
4. Now that you have trained your kinky model you can go to the results folder and run
   ``portfolioML.results.model_predict``. It will return a csv file that contains the forecasts and use it to
   generate a ROC curve (Receiver operating characteristic curve). You have almost done.
5. Finally, the module ``portfolioML.results.portfolio_creation`` keeps track of the trading days,
   creates all the portfolios on that days based on long-top-k and short-bottom-k companies and
   saves some images related to returns distribution and accumulative returns compared with results
   of a randomness test.

.. note::
   For ``portfolioML.results.portfolio_creation`` the correct syntax to run it is

   .. code-block:: python

      python portfolio_creation.py --algorithm=<algorithm1> --algorithm=<algorithm2> --model_name=<model_name1> --model_name=<model_name2> --num_periods=<num_periods>

   where you can put as much algorithm (LSTM,CNN,DNN,RAF) as you want but the order must match with model_name.
   For example, if you want to obtain the accumulative and distribution returns of CNN and RAF respect to the particular
   models CNN_dense and RAF_Model1 over all the 17 periods, run the following

   .. code-block:: bash

      python portfolio_creation.py --algorithm=CNN --algorithm=RAF --model_name=CNN_dense --model_name=RAF_Model1 --num_periods=17




.. warning::
    The majority of the modules is strictly constrained on their path, since they pick and/or
    create folders and files starting from their position. Keep in mind to run them from their
    folder.

The following represents the tree structure of the folder and the main files


.. code-block:: bash

   ├── docs
   ├── LICENSE
   ├── portfolioML
   │   ├── data
   │   │   ├── after_test.csv
   │   │   ├── after_train.csv
   │   │   ├── autoencoder_cnn.py
   │   │   ├── autoencoders.py
   │   │   ├── data_generator.py
   │   │   ├── data_returns.py
   │   │   ├── data_visualization.py
   │   │   ├── MultidimReturnsData1.csv
   │   │   ├── MultidimReturnsData2.csv
   │   │   ├── MultidimReturnsData3.csv
   │   │   ├── MultidimReturnsData4.csv
   │   │   ├── preprocessing.py
   │   │   ├── PriceData.csv
   │   │   ├── ReturnsBinary.csv
   │   │   ├── ReturnsBinaryPCA.csv
   │   │   ├── ReturnsData.csv
   │   │   └── ReturnsDataPCA.csv
   │   ├── makedir.py
   │   ├── model
   │   │   ├── CNN
   │   │   │   ├── CNN_dense
   │   │   │   ├── CNN_dense+
   │   │   │   ├── CNN_dense2_plus
   │   │   │   ├── CNN_dense_pca_wave
   │   │   │   ├── CNN_minpool
   │   │   │   └── cnn.py
   │   │   ├── DNN
   │   │   │   ├── DNN_mymod2
   │   │   │   ├── DNN_mymod2_pca
   │   │   │   ├── DNN_mymod4
   │   │   │   ├── DNN_paper
   │   │   │   ├── DNN_paper_auto
   │   │   │   ├── DNN_paper_pca
   │   │   │   └── dnn.py
   │   │   ├── LSTM
   │   │   │   ├── LSTM_Model1
   │   │   │   ├── LSTM_Model2
   │   │   │   ├── LSTM_Model4
   │   │   │   └── lstm.py
   │   │   ├── preprocessing_ang.py
   │   │   ├── RAF
   │   │   │   └── raf.py
   │   │   └── split.py
   │   ├── results
   │   │   ├── model_predict.py
   │   │   ├── portfolio_creation.py
   │   │   ├── predictions
   │   │   │   ├── CNN
   │   │   │   ├── DNN
   │   │   │   ├── LSTM
   │   │   │   └── RAF
   │   │   ├── predictions_for_portfolio
   │   │   │   ├── CNN
   │   │   │   ├── DNN
   │   │   │   ├── LSTM
   │   │   │   └── RAF
   │   │   ├── ROC
   │   │   │   ├── CNN
   │   │   │   ├── DNN
   │   │   │   ├── LSTM
   │   │   │   └── RAF
   │   └── tests
   ├── README.md
   ├── requirements.txt
   └── setup.py
