##############################################################################
# Import necessary modules and files
# #############################################################################


import pandas as pd
import os
import sqlite3
from sqlite3 import Error
from constants import *
from mapping.significant_categorical_level import *
from mapping.city_tier_mapping import *


###############################################################################
# Define the function to build database
# ##############################################################################

def build_dbs():
    '''
    This function checks if the db file with specified name is present 
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates 
    the db file with the given name at the given path. 


    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should exist  


    OUTPUT
    The function returns the following under the given conditions:
        1. If the file exists at the specified path
                prints 'DB Already Exists' and returns 'DB Exists'

        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db 
                file at the specified path with the specified name and 
                once the db file is created prints 'New DB Created' and 
                returns 'DB created'


    SAMPLE USAGE
        build_dbs()
    '''
    if os.path.isfile(DB_PATH + DB_FILE_NAME):
        print( "DB Already Exsist")
        print(os.getcwd())
        return "DB Exsist"
    else:
        print ("Creating Database")
        """ create a database connection to a SQLite database """
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
            print("New DB Created")
        except Error as e:
            print(e)
            return "Error"
        finally:
            if conn:
                conn.close()
                return "DB Created"

###############################################################################
# Define function to load the csv file to the database
# ##############################################################################

def load_data_into_db():
    '''
    Thie function loads the data present in data directory into the db
    which was created previously.
    It also replaces any null values present in 'toal_leads_dropped' and
    'referred_lead' columns with 0.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        

    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function 
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    '''
    
    data = pd.read_csv(DATA_DIRECTORY + DATA_FILE)
    conn = None
    
    if 'Unnamed: 0' in data.columns:
        data = data.drop(columns = 'Unnamed: 0', axis = 1)
    
    if 'toal_leads_dropped' in data.columns:
        data['toal_leads_dropped'].fillna(0)
    
    if 'referred_lead' in data.columns:
        data['referred_lead'].fillna(0)
    
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        data.to_sql(name = 'loaded_data', con = conn, if_exists = 'replace', index = False)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


###############################################################################
# Define function to map cities to their respective tiers
# ##############################################################################

    
def map_city_tier():
    '''
    This function maps all the cities to their respective tier as per the
    mappings provided in the city_tier_mapping.py file. If a
    particular city's tier isn't mapped(present) in the city_tier_mapping.py 
    file then the function maps that particular city to 3.0 which represents
    tier-3.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier

    
    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_city_tier()

    '''
    
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        data = pd.read_sql('select * from loaded_data', conn)
        data['city_tier'] = data['city_mapped'].map(city_tier_mapping)
        data['city_tier'] = data['city_tier'].fillna(3.0)
        # remove non required features
        data = data.drop(NOT_FEATURES, axis = 1)
        data.to_sql(name = 'city_tier_mapped', con = conn, if_exists = 'replace', index = False)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


###############################################################################
# Define function to map insignificant categorial variables to "others"
# ##############################################################################


def map_categorical_vars():
    '''
    This function maps all the insignificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py' 
    so that it can be imported as a variable in utils file.
    

    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source

        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.
  

    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_categorical_vars()
    '''
    
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        data = pd.read_sql('select * from city_tier_mapped', conn)
        data = map_categorical_vars_util(data, list_platform, 'first_platform_c')
        data = map_categorical_vars_util(data, list_medium, 'first_utm_medium_c')
        data = map_categorical_vars_util(data, list_source, 'first_utm_source_c')
        data.to_sql(name = 'categorical_variables_mapped', con = conn, if_exists = 'replace', index = False)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
        


##############################################################################
# Define function that maps interaction columns into 4 types of interactions
# #############################################################################
def interactions_mapping():
    '''
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file. 


    INPUTS
        DB_FILE_NAME: Name of the database file
        DB_PATH : path where the db file should be present
        INTERACTION_MAPPING : path to the csv file containing interaction's
                                   mappings
        INDEX_COLUMNS_TRAINING : list of columns to be used as index while pivoting and
                                 unpivoting during training
        INDEX_COLUMNS_INFERENCE: list of columns to be used as index while pivoting and
                                 unpivoting during inference
        NOT_FEATURES: Features which have less significance and needs to be dropped
                                 
        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exculde it from our features list. It is recommended 
        that you use an if loop and check if 'app_complete_flag' is present in 
        'categorical_variables_mapped' table and if it is present pass a list with 
        'app_complete_flag' column, or else pass a list without 'app_complete_flag'
        column.

    
    OUTPUT
        Saves the processed dataframe in the db in a table named 
        'interactions_mapped'. If the table with the same name already exsists then 
        the function replaces it.
        
        It also drops all the features that are not requried for training model and 
        writes it in a table named 'model_input'

    
    SAMPLE USAGE
        interactions_mapping()
    '''
    
    df_event_mapping = pd.read_csv(INTERACTION_MAPPING, index_col=[0])
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        data = pd.read_sql('select * from categorical_variables_mapped', conn)
        data = interactions_mapping_util(data, df_event_mapping)
        data.to_sql(name = 'interactions_mapped', con = conn, if_exists = 'replace', index = False)
        
        # save data for training model
        data.to_sql(name = 'model_input', con = conn, if_exists = 'replace', index = False)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
        
    

################################# private utilities function ##############################

def map_categorical_vars_util(df, categories, col_name):
    new_df = df[~df[col_name].isin(categories)]
    new_df[col_name] = 'others'
    old_df = df[df[col_name].isin(categories)]
    return pd.concat([new_df, old_df])


def interactions_mapping_util(df, df_event_mapping):
    # drop the duplicates rows
    df = df.drop_duplicates()
    pivot_cols = []
    
    if (DATA_FILE == '/leadscoring.csv'):
        pivot_cols = INDEX_COLUMNS_TRAINING
    else:
        pivot_cols = INDEX_COLUMNS_INFERENCE
    
    # unpivot the interaction columns and put the values in rows
    df_unpivot = pd.melt(df, id_vars=pivot_cols, var_name='interaction_type', value_name='interaction_value')
    
    # handle the nulls in the interaction value column
    df_unpivot['interaction_value'] = df_unpivot['interaction_value'].fillna(0)
    
    # map interaction type column with the mapping file to get interaction mapping
    df = pd.merge(df_unpivot, df_event_mapping, on='interaction_type', how='left')
    
    #dropping the interaction type column as it is not needed
    df = df.drop(['interaction_type'], axis=1)
    
    # pivoting the interaction mapping column values to individual columns in the dataset
    df_pivot = df.pivot_table(values='interaction_value', index=pivot_cols, columns='interaction_mapping', aggfunc='sum')
    
    df_pivot = df_pivot.reset_index()
    return df_pivot

def fetch_data(table_name):
    conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
    data = pd.read_sql(f'select * from {table_name}', conn)
    return data
    
