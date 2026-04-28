"""Generic functions for handling ETL transactions with a few database types.
Currently implemented:
    - SQL Server (extract and load)
    - Oracle DB (extract)
    - SQLite (extract)
"""
from datetime import datetime
from itertools import groupby
import logging
import os
import sqlite3 as sqlite

import oracledb
import pandas as pd
import pyodbc
import requests

######################################################################################################
# EXTRACT
######################################################################################################

# Keep in mind that in ODBC parameters are passed sequentially, so the query will need to have as many ? placeholders
# as it has parameters that get passed (I believe this is also the case for repeat parameters). Could probably
# write some logic to make sure that this doesn't become a problem, but I'll save that for later.
def extract_from_sql_server(server, user, password, query, parameters = None):
    connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};UID={user};PWD={password};Authentication=SqlPassword;TrustServerCertificate=yes;'
    try:
        with pyodbc.connect(connection_string) as connection:
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
# get passed are called for, but I can take care of those later. Lots of fun for future me! 
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

def get_api_data(url, username, password, parameters = {}):
    """Wrapper function for retrieving data (in json format) from an API via HTTP.

    Args:
        url(str): the API url to send the request to.
        username(str): username for API authentication.
        password(str): password for API authentication.
        parameters(dict, empty by default): dictionary of key/value pairs to be passed as parameters to the request.

    Returns:
        dict: HTTP response content in json format.
    """
    if parameters:
        response = requests.get(url, auth = (username, password), params = parameters)
    else:
        response = requests.get(url, auth = (username, password))
    return response.json()

######################################################################################################
# TRANSFORM
######################################################################################################

def json_to_dataframe(json):
    """Convert an HTTP response (in json format) containing tabular data into a DataFrame.

    Args:
        json(dict): an HTTP response (from the requests module) in json format.

    Returns:
        pandas.DataFrame: a dataframe containing the response data.
    """
    rows = json['elements']
    
    # Confirm that all rows of response have the same key names
    # (exclude hyperlinks that are included in the response, since they contain no relevant data)
    all_keys = [[key for key in row.keys() if key != 'links'] for row in rows]
    def all_equal(iterable):
        """Determine whether all elements in an iterable are equal to one another
        Borrowed from this Stack Overflow answer by user kennytm: https://stackoverflow.com/a/3844832/27800403
        """
        g = groupby(iterable)
        return next(g, True) and not next(g, False)
    if not all_equal(all_keys):
        raise ValueError(f'Not all elements in json response have the same keys. Try confirming that the data in the response is actually tabular.')
    
    column_names = all_keys[0]
    columns = {}
    for column_name in column_names:
        column_data = [row[column_name] for row in rows] # Get all data with same column name in single list
        columns[column_name] = column_data
    dataframe = pd.DataFrame.from_dict(columns)
    return dataframe

def query_results_to_dataframe(query_results, column_names):
    return pd.DataFrame.from_records(query_results, columns = column_names)

def preprocess_dataframe(dataframe, load_id, column_names):
    """A bunch of preprocessing steps that I was already doing for each part of the data
    load anyways.
        - Checks column names and re-orders columns where called for
        - Replaces missing values with empty strings
        - Adds a "load_id" column
        - Adds an "insert_timestamp" column
        - Re-orders columns so that load_id is first and insert_timestamp is last.

    Args:
        dataframe (pandas.DataFrame): a pandas DataFrame.
        load_id (int): load_id number, as retrieved by get_load_id, to be inserted into the table.
        column_names (list[str]): names of columns in dataframe before preprocessing, in intended order

    Returns:
        pandas.DataFrame: preprocessed data.
    """
    columns = list(dataframe.columns)
    # Just a little excessive data validation nothing to see here
    if sorted(columns) != sorted(column_names):
        if len(columns) > len(column_names):
            error_message = 'The passed dataframe has more columns than column names specified.'
        elif len(columns) < len(column_names):
            error_message = 'The passed dataframe has fewer columns than column names specified.'
        else:
            error_message = 'The column names in the passed dataframe do not match the column names specified.'
        raise ValueError(error_message)
    output = dataframe[column_names]
    output.fillna('', inplace = True)
    output['load_id'] = load_id
    output['insert_timestamp'] = datetime.now()
    output = output[['load_id'] + column_names + ['insert_timestamp']]
    return output

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
    connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};UID={user};PWD={password};Authentication=SqlPassword;TrustServerCertificate=yes;'
    try:
        with pyodbc.connect(connection_string) as connection:
            with connection.cursor() as cursor:
                cursor.executemany(query, parameters)
    except Exception as e:
        logging.error(f'An exception was raised while connecting to SQL server {server}: {str(e)}')
        raise
