##############################################################################
# Import necessary modules
# #############################################################################
import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils import *


###############################################################################
# Define default arguments and create an instance of DAG
# ##############################################################################

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022,7,30),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


Lead_scoring_inference_dag = DAG(
                dag_id = 'Lead_scoring_inference_pipeline',
                default_args = default_args,
                description = 'Inference pipeline of Lead Scoring system',
                schedule_interval = '@hourly',
                catchup = False
)

###############################################################################
# Create a task for encode_data_task() function with task_id 'encoding_categorical_variables'
# ##############################################################################
op_encode_features = PythonOperator(task_id = 'encoding_categorical_variables',
                                    python_callable = encode_features,
                                    dag = Lead_scoring_inference_dag)


###############################################################################
# Create a task for load_model() function with task_id 'generating_models_prediction'
# ##############################################################################
op_load_model = PythonOperator(task_id = 'generating_models_prediction',
                               python_callable = load_model,
                               dag = Lead_scoring_inference_dag)


###############################################################################
# Create a task for prediction_col_check() function with task_id 'checking_model_prediction_ratio'
# ##############################################################################
op_prediction_col_check = PythonOperator(task_id = 'checking_model_prediction_ratio',
                                         python_callable = prediction_col_check,
                                         dag = Lead_scoring_inference_dag)


###############################################################################
# Create a task for input_features_check() function with task_id 'checking_input_features'
# ##############################################################################
op_input_features_check = PythonOperator(task_id = 'checking_input_features',
                                         python_callable = input_col_check,
                                         dag = Lead_scoring_inference_dag)


###############################################################################
# Define relation between tasks
# ##############################################################################
op_encode_features.set_downstream(op_input_features_check)
op_input_features_check.set_downstream(op_load_model)
op_load_model.set_downstream(op_prediction_col_check)
