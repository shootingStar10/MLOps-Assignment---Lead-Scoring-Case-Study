DB_PATH = 'airflow/dags/lead_scoring_datapipeline/data/database'

DB_FILE_NAME = '/lead_scoring_data_cleaning.db'

DB_FILE_MLFLOW = 'Lead_scoring_mlflow_production.db'

MLFLOW_PATH = 'Assignment/mlruns/0/354d8b5a7bad4594a09673e211e38ac1/artifacts/models'

FILE_PATH = 'airflow/dags/lead_scoring_inference_pipeline'

TRACKING_URI = 'http://0.0.0.0:6006'

# experiment, model name and stage to load the model from mlflow model registry
MODEL_NAME = 'LightGBM'
STAGE = 'Production'
EXPERIMENT = 'Lead_scoring_mlflow_production'

# list of the features that needs to be there in the final encoded dataframe
ONE_HOT_ENCODED_FEATURES = ['total_leads_droppped', 
                            'city_tier', 
                            'referred_lead', 
                            'first_platform_c', 
                            'first_utm_medium_c', 
                            'first_utm_source_c']

# list of features that need to be one-hot encoded
FEATURES_TO_ENCODE = ['first_platform_c', 
                      'first_utm_medium_c', 
                      'first_utm_source_c']
