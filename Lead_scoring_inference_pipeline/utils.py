"""
Import necessary modules
##############################################################################
"""

import mlflow
import mlflow.sklearn
import pandas as pd

import sqlite3
from sqlite3 import Error

from sklearn.preprocessing import LabelEncoder

import os
import logging

from datetime import datetime
from constants import *

###############################################################################
# Define the function to train the model
# ##############################################################################


def encode_features():
    '''
    This function one hot encodes the categorical features present in our  
    training dataset. This encoding is needed for feeding categorical data 
    to many scikit-learn models.

    INPUTS
        db_file_name : Name of the database file 
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES : list of the features that needs to be there in the final encoded dataframe
        FEATURES_TO_ENCODE: list of features  from cleaned data that need to be one-hot encoded
        **NOTE : You can modify the encode_featues function used in heart disease's inference
        pipeline for this.

    OUTPUT
        1. Save the encoded features in a table - features

    SAMPLE USAGE
        encode_features()
    '''
    
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        data = pd.read_sql('select * from model_input', conn)
        # encoding the categories
        encoder = LabelEncoder()
        for col in FEATURES_TO_ENCODE:
            data[col] = encoder.fit_transform(data[col])

        # update data by taking only ONE_HOT_ENCODED_FEATURES
        features_data = data[ONE_HOT_ENCODED_FEATURES]

        # save data to db
        features_data.to_sql(name = 'features', con = conn, if_exists = 'replace', index = False)
    except Error as e:
        print('Error: ', e)
        return
    finally:
        if conn:
            conn.close()

###############################################################################
# Define the function to load the model from mlflow model registry
# ##############################################################################

def load_model():
    '''
    This function loads the model which is in production from mlflow registry and 
    uses it to do prediction on the input dataset. Please note this function will the load
    the latest version of the model present in the production stage. 

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        model from mlflow model registry
        model name: name of the model to be loaded
        stage: stage from which the model needs to be loaded i.e. production


    OUTPUT
        Store the predicted values along with input data into a table

    SAMPLE USAGE
        load_model()
    '''
    
    mlflow.set_tracking_uri(TRACKING_URI)
    model = mlflow.sklearn.load_model(MLFLOW_PATH)
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        X = pd.read_sql('select * from features', conn)
        preds = model.predict(X)
        predictions = pd.DataFrame({'predictions': preds})
        predictions.to_sql(name = 'predictions', con = conn, if_exists = 'replace', index = False)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
        

###############################################################################
# Define the function to check the distribution of output column
# ##############################################################################

def prediction_col_check():
    '''
    This function calculates the % of 1 and 0 predicted by the model and  
    and writes it to a file named 'prediction_distribution.txt'.This file 
    should be created in the ~/airflow/dags/Lead_scoring_inference_pipeline 
    folder. 
    This helps us to monitor if there is any drift observed in the predictions 
    from our model at an overall level. This would determine our decision on 
    when to retrain our model.
    

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be

    OUTPUT
        Write the output of the monitoring check in prediction_distribution.txt with 
        timestamp.

    SAMPLE USAGE
        prediction_col_check()
    '''
    
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        predictions = pd.read_sql('select * from predictions', conn)
        ones = sum(predictions['predictions'])
        zeros = predictions.shape[0] - ones
        with open(FILE_PATH + '/prediction_distribution.txt', 'a') as file:
            file.write(f'Zeros % = {round((zeros / predictions.shape[0])*100, 2)}, Ones % = {round((ones / predictions.shape[0])*100, 2)}\n')
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
        

###############################################################################
# Define the function to check the columns of input features
# ##############################################################################


def input_col_check():
    '''
    This function checks whether all the input columns are present in our new
    data. This ensures the prediction pipeline doesn't break because of change in
    columns in input data.

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES: List of all the features which need to be present
        in our input data.

    OUTPUT
        It writes the output in a log file based on whether all the columns are present
        or not.
        1. If all the input columns are present then it logs - 'All the models input are present'
        2. Else it logs 'Some of the models inputs are missing'

    SAMPLE USAGE
        input_col_check()
    '''
    
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        X = pd.read_sql('select * from features', conn)
        missing = False
        cols = []
        
        for col in ONE_HOT_ENCODED_FEATURES:
            if col not in X.columns:
                missing = True
                break
                
        with open(FILE_PATH + '/logs.txt', 'a') as file:
            if missing == True:
                file.write(f'LOG {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: Some of the models inputs are missing\n')
            else:
                file.write(f'LOG {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: All the models input are present\n')
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
