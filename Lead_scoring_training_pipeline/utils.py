"""
Import necessary modules
##############################################################################
"""

import pandas as pd
import numpy as np

import sqlite3
from sqlite3 import Error

import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, accuracy_score
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

from constants import *


###############################################################################
# Define the function to encode features
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
       

    OUTPUT
        1. Save the encoded features in a table - features
        2. Save the target variable in a separate table - target


    SAMPLE USAGE
        encode_features()
        
    **NOTE : You can modify the encode_featues function used in heart disease's inference
        pipeline from the pre-requisite module for this.
    '''
    
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        data = pd.read_sql('select * from model_input', conn)
        # encoding the categories
        encoder = LabelEncoder()
        for col in FEATURES_TO_ENCODE:
            data[col] = encoder.fit_transform(data[col])

        # update data by taking only ONE_HOT_ENCODED_FEATURES
        data = data[ONE_HOT_ENCODED_FEATURES]

        # derive features data and target data
        features_data = data.drop(columns = 'app_complete_flag', axis = 1)
        target_data = data['app_complete_flag']

        # save data to db
        features_data.to_sql(name = 'features', con = conn, if_exists = 'replace', index = False)
        target_data.to_sql(name = 'target', con = conn, if_exists = 'replace', index = False)
    except Error as e:
        print('Error: ', e)
        return
    finally:
        if conn:
            conn.close()


###############################################################################
# Define the function to train the model
# ##############################################################################

def get_trained_model():
    '''
    This function setups mlflow experiment to track the run of the training pipeline. It 
    also trains the model based on the features created in the previous function and 
    logs the train model into mlflow model registry for prediction. The input dataset is split
    into train and test data and the auc score calculated on the test data and
    recorded as a metric in mlflow run.   

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be


    OUTPUT
        Tracks the run in experiment named 'Lead_Scoring_Training_Pipeline'
        Logs the trained model into mlflow model registry with name 'LightGBM'
        Logs the metrics and parameters into mlflow run
        Calculate auc from the test data and log into mlflow run  

    SAMPLE USAGE
        get_trained_model()
    '''
    
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        X = pd.read_sql('select * from features', conn)
        y = pd.read_sql('select * from target', conn)
    except Error as e:
        print('Error: ', e)
        return
    finally:
        if conn:
            conn.close()
    
    # split data into train and test
    X_train, X_test, y_train, y_test = train_test_split(X, y, train_size = 0.8, random_state = 100)
    
    mlflow.set_tracking_uri(TRACKING_URI)
    
    # setting mlflow
    with mlflow.start_run(run_name = EXPERIMENT) as run:
        # training model
        clf = lgb.LGBMClassifier()
        clf.set_params(**model_config) 
        clf.fit(X_train, y_train)
        
        # log model
        mlflow.sklearn.log_model(sk_model = clf, artifact_path = "models", registered_model_name = 'LightGBM')
        mlflow.log_params(model_config)
        
        # predict the results on training dataset
        y_train_pred = clf.predict(X_train)

        # predict the results on testing dataset
        y_test_pred = clf.predict(X_test)
        
        # get metrics for train
        train_accuracy = accuracy_score(y_train, y_train_pred)
        train_auc = roc_auc_score(y_train, y_train_pred)
        
        # get metrics for test
        test_accuracy = accuracy_score(y_test, y_test_pred)
        test_auc = roc_auc_score(y_test, y_test_pred)
        
        # log metrics
        mlflow.log_metric('Train Accuracy', train_accuracy)
        mlflow.log_metric('Train AUC score', train_auc)
        mlflow.log_metric('Test Accuracy', test_accuracy)
        mlflow.log_metric('Test AUC score', test_auc)
        
        runID = run.info.run_uuid
        print("Inside MLflow Run with id {}".format(runID))
        
        
def fetch_data():
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        X = pd.read_sql('select * from features', conn)
        y = pd.read_sql('select * from target', conn)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    
    return X, y
