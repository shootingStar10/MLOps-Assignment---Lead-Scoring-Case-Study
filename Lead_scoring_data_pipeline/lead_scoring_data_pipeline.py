##############################################################################
# Import necessary modules
# #############################################################################
import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from data_validation_checks import *
from utils import *
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


###############################################################################
# Define default arguments and DAG
# ##############################################################################

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,12,15),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


ML_data_cleaning_dag = DAG(
                dag_id = 'Lead_Scoring_Data_Engineering_Pipeline',
                default_args = default_args,
                description = 'DAG to run data pipeline for lead scoring',
                schedule_interval = '@daily',
                catchup = False
)

###############################################################################
# Create a task for build_dbs() function with task_id 'building_db'
# ##############################################################################

op_build_dbs = PythonOperator(task_id = 'building_db',
                              python_callable = build_dbs,
                              dag = ML_data_cleaning_dag)

###############################################################################
# Create a task for raw_data_schema_check() function with task_id 'checking_raw_data_schema'
# ##############################################################################

op_raw_data_schema_check = PythonOperator(task_id = 'checking_raw_data_schema',
                                          python_callable = raw_data_schema_check,
                                          dag = ML_data_cleaning_dag)

###############################################################################
# Create a task for load_data_into_db() function with task_id 'loading_data'
# #############################################################################

op_load_data_into_db = PythonOperator(task_id = 'loading_data',
                                      python_callable = load_data_into_db,
                                      dag = ML_data_cleaning_dag)

###############################################################################
# Create a task for map_city_tier() function with task_id 'mapping_city_tier'
# ##############################################################################

op_map_city_tier = PythonOperator(task_id = 'mapping_city_tier',
                                  python_callable = map_city_tier,
                                  dag = ML_data_cleaning_dag)

###############################################################################
# Create a task for map_categorical_vars() function with task_id 'mapping_categorical_vars'
# ##############################################################################

op_map_categorical_vars = PythonOperator(task_id = 'mapping_categorical_vars',
                                         python_callable = map_categorical_vars,
                                         dag = ML_data_cleaning_dag)

###############################################################################
# Create a task for interactions_mapping() function with task_id 'mapping_interactions'
# ##############################################################################

op_interactions_mapping = PythonOperator(task_id = 'mapping_interactions',
                                         python_callable = interactions_mapping,
                                         dag = ML_data_cleaning_dag)

###############################################################################
# Create a task for model_input_schema_check() function with task_id 'checking_model_inputs_schema'
# ##############################################################################

op_model_input_schema_check = PythonOperator(task_id = 'checking_model_inputs_schema',
                                          python_callable = model_input_schema_check,
                                          dag = ML_data_cleaning_dag)

###############################################################################
# Define the relation between the tasks
# ##############################################################################

op_build_dbs.set_downstream(op_raw_data_schema_check)
op_raw_data_schema_check.set_downstream(op_load_data_into_db)
op_load_data_into_db.set_downstream(op_map_city_tier)
op_map_city_tier.set_downstream(op_map_categorical_vars)
op_map_categorical_vars.set_downstream(op_interactions_mapping)
op_interactions_mapping.set_downstream(op_model_input_schema_check)
