# You can create more variables according to your project. The following are the basic variables that have been provided to you
DB_PATH = ''

DB_FILE_NAME = './utils_output.db'

UNIT_TEST_DB_FILE_NAME = './unit_test_cases.db'

DATA_DIRECTORY = './data'

INTERACTION_MAPPING = './interaction_mapping.csv'

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
                          'referred_lead', 
                          'app_complete_flag']

NOT_FEATURES = ['city_mapped']




