import logging
import os
import sqlite3 as sqlite

import oracledb
import pandas as pd
import pyodbc

######################################################################################################
# EXTRACT
######################################################################################################

# Keep in mind that in ODBC parameters are passed sequentially, so the query will need to have as many ? placeholders
# as it has parameters that get passed (I believe this is also the case for repeat parameters). Could probably
# write some logic to make sure that this doesn't become a problem, but I'll save that for later.
def extract_from_sql_server(server, user, password, query, parameters = None):
    connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};UID={user};PWD={password};TrustServerCertificate=yes;'
    try:
        with pyodbc.connect(connection_string, trusted_connection = 'Yes') as connection:
            with connection.cursor() as cursor:
                if parameters:
                    cursor.execute(query, parameters)
                    logging.info(f'Executed query: {query} with parameters {parameters}')
                else:
                    cursor.execute(query)
                    logging.info(f'Executed query: {query}')
                return_data = cursor.fetchall()
    except Exception as e:
        logging.error(f'An exception was raised while connecting to SQL Server {server}: {str(e)}')
    return return_data

# cursor.fetchall() returns a list of rows, with row being a tuple-like data type from the pyodbc package
# (see https://github.com/mkleehammer/pyodbc/wiki/Getting-started for details)

# oracledb can use both named and positional parameters; I use positional parameters here for consistency
# with the SQL Server extract method. As above, some checks to ensure that the correct number of parameters
# are called for, but I can take care of those later. Lots of fun for future me! 
def extract_from_oracle(dsn, user, password, query, parameters = None):
    oracledb.init_oracle_client()
    try:
        with oracledb.connect(user = user, password = password, dsn = dsn) as connection:
            with connection.cursor() as cursor:
                if parameters:
                    cursor.execute(query, parameters)
                    logging.info(f'Executed query: {query} with parameters {parameters}')
                else:
                    cursor.execute(query)
                    logging.info(f'Executed query: {query}')
                return_data = cursor.fetchall()
    except Exception as e:
        logging.error(f'An exception was raised while connecting to Oracle DB {dsn}: {str(e)}')
        raise
    return return_data

# Not concerned about parameterized queries for SQLite since I won't be using any for this project.
def extract_from_sqlite(path_to_db, query):
    try:
        if not os.path.exists(path_to_db):
            raise ValueError(f'No database file found at {path_to_db}')
    except Exception as e:
        logging.error(str(e))
        raise
    connection = sqlite.connect(path_to_db)
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        return_data = cursor.fetchall()
    except Exception as e:
       logging.error(f'An exception was raised while querying SQLite database at {path_to_db}: {str(e)}')
    return return_data

######################################################################################################
# TRANSFORM
######################################################################################################

def query_results_to_dataframe(query_results, column_names):
    return pd.DataFrame.from_records(query_results, columns = column_names)

def parameterize_data_frame(df: pd.DataFrame):
    """Converts rows of a DataFrame to a list of tuples ready to be used as parameters for a 
    bulk insert statement.

    Args:
        df (pd.DataFrame): a DataFrame.
    
    Returns
        [(str, ...), ...]: a list of tuples, each representing a row of data.
    """
    return [tuple(row) for row in df.itertuples(index = False)]

######################################################################################################
# LOAD
######################################################################################################

# In this case the parameters are tuples of insert data, and the tuples should all be contained
# in a list so a bulk insert can be performed. I've written a convenience function for this
# (parameterize_data_frame(), above) since all of the data in this project will be passed through
# a DataFrame at some point, but more caution will need to be used when passing parameters in more
# generic use cases in the future.

def insert_to_sql_server(server, user, password, query, parameters = None):
    connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};UID={user};PWD={password};TrustServerCertificate=yes;'
    try:
        with pyodbc.connect(connection_string, trusted_connection = 'Yes') as connection:
            with connection.cursor() as cursor:
                cursor.executemany(query, parameters)
    except Exception as e:
        logging.error(f'An exception was raised while connecting to SQL server {server}: {str(e)}')
        raise
