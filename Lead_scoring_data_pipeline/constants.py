# You can create more variables according to your project. The following are the basic variables that have been provided to you
DB_PATH = 'airflow/dags/lead_scoring_datapipeline/data/database'

DB_FILE_NAME = '/lead_scoring_data_cleaning.db'

UNIT_TEST_DB_FILE_NAME = '/unit_test_cases.db'

DATA_DIRECTORY = 'airflow/dags/lead_scoring_datapipeline/data'

INTERACTION_MAPPING = 'airflow/dags/lead_scoring_datapipeline/mapping/interaction_mapping.csv'

INDEX_COLUMNS_TRAINING = ['created_date', 
                          'first_platform_c',
                          'first_utm_medium_c', 
                          'first_utm_source_c', 
                          'total_leads_droppped', 
                          'city_tier',
                          'referred_lead', 
                          'app_complete_flag']

INDEX_COLUMNS_INFERENCE = ['created_date', 
                          'first_platform_c',
                          'first_utm_medium_c', 
                          'first_utm_source_c', 
                          'total_leads_droppped', 
                          'city_tier',
                          'referred_lead']

NOT_FEATURES = ['city_mapped']

DATA_FILE = '/leadscoring_inference.csv'




