##############################################################################
# Import the necessary modules
# #############################################################################

import sqlite3
from sqlite3 import Error
from utils import *
from constants import *

###############################################################################
# Write test cases for load_data_into_db() function
# ##############################################################################

def test_load_data_into_db():
    """_summary_
    This function checks if the load_data_into_db function is working properly by
    comparing its output with test cases provided in the db in a table named
    'loaded_data_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_get_data()

    """
    
    expected_data = None
    actual_data = None
    
    try:
        conn = sqlite3.connect(DB_PATH + UNIT_TEST_DB_FILE_NAME)
        expected_data = pd.read_sql('select * from loaded_data_test_case', conn)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    
    load_data_into_db()
    
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        actual_data = pd.read_sql('select * from loaded_data', conn)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    
    assert actual_data is not None
    assert expected_data is not None
    assert actual_data.shape == expected_data.shape
    assert sorted(actual_data.columns) == sorted(expected_data.columns)


###############################################################################
# Write test cases for map_city_tier() function
# ##############################################################################
def test_map_city_tier():
    """_summary_
    This function checks if map_city_tier function is working properly by
    comparing its output with test cases provided in the db in a table named
    'city_tier_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_map_city_tier()

    """
    
    expected_data = None
    actual_data = None
    
    try:
        conn = sqlite3.connect(DB_PATH + UNIT_TEST_DB_FILE_NAME)
        expected_data = pd.read_sql('select * from city_tier_mapped_test_case', conn)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    
    map_city_tier()
    
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        actual_data = pd.read_sql('select * from city_tier_mapped', conn)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    
    assert actual_data is not None
    assert expected_data is not None
    assert actual_data.shape == expected_data.shape
    assert sorted(actual_data.columns) == sorted(expected_data.columns)
    

###############################################################################
# Write test cases for map_categorical_vars() function
# ##############################################################################    
def test_map_categorical_vars():
    """_summary_
    This function checks if map_cat_vars function is working properly by
    comparing its output with test cases provided in the db in a table named
    'categorical_variables_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'
    
    SAMPLE USAGE
        output=test_map_cat_vars()

    """
    
    expected_data = None
    actual_data = None
    
    try:
        conn = sqlite3.connect(DB_PATH + UNIT_TEST_DB_FILE_NAME)
        expected_data = pd.read_sql('select * from categorical_variables_mapped_test_case', conn)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    
    map_categorical_vars()
    
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        actual_data = pd.read_sql('select * from categorical_variables_mapped', conn)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    
    assert actual_data is not None
    assert expected_data is not None
    assert actual_data.shape == expected_data.shape
    assert sorted(actual_data.columns) == sorted(expected_data.columns)
    
    
################################################################################
# Write test cases for interactions_mapping() function
# ##############################################################################    
def test_interactions_mapping():
    """_summary_
    This function checks if test_column_mapping function is working properly by
    comparing its output with test cases provided in the db in a table named
    'interactions_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_column_mapping()

    """
    
    expected_data = None
    actual_data = None
    
    try:
        conn = sqlite3.connect(DB_PATH + UNIT_TEST_DB_FILE_NAME)
        expected_data = pd.read_sql('select * from interactions_mapped_test_case', conn)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    
    interactions_mapping()
    
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        actual_data = pd.read_sql('select * from interactions_mapped', conn)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
    
    assert actual_data is not None
    assert expected_data is not None
    assert actual_data.shape == expected_data.shape
    assert sorted(actual_data.columns) == sorted(expected_data.columns)
