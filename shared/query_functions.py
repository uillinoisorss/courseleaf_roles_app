import logging
import os
import re

import pyodbc
import yaml

# Run a SQL Server query without returning anything.
# Primarily intended for running truncate queries and the like.
def run_sql_server_query(server, user, password, query, parameters = None):
    """Execute a query against a SQL Server instance without returning any data. 
    Optionally takes a list of parameters to be passed to the query. Primarily intended
    for use with DDL and DML queries.

    Args:
        server (str): SQL server instance connection string.
        user (str): username for SQL Server login.
        password (str): password for SQL Server login.
        query (str): query to be run against the SQL server instance.
        parameters (list(str), default None): list of parameters to be passed alongside the query. If not provided,
            then the query is exectuted as though it has no parameters, which can cause problems if the query
            actually does take parameters.
    """
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
    except Exception as e:
        logging.error(f'An exception was raised while connecting to SQL Server {server}: {str(e)}')
        raise

# Functions for creating SQL query YAML file

def strip_query(path, drop_semicolon = False, param_prefix = None):
    """Prepares the contents of a .sql file to be inserted into a YAML file by removing contents and 
    replacing all whitespace with a single space, collapsing the query to a single line.
    Has optional parameters for configuring the final SQL string format.

    Args:
        path (path): path to the .sql file to read from.
        drop_semicolon (bool, default False): whether to remove semicolons from the end of the SQL string.
        param_prefix (str, default None): character that will be used to prefix query parameters, replacing
            any existing parameter prefix from a pre-defined list (right now just &). If no value is 
            supplied, then no prefix substitution takes place.

    Returns:
        str: the processed query string.
    """
    try:
        with open(path) as file:
            lines = file.readlines()
    except Exception as e:
        logging.error(f'An exception was raised while attempting to access the directory {path}: {str(e)}')
        raise
    lines = [re.sub(r'--+.*', '', line) for line in lines]
    query = ''.join(lines)
    if drop_semicolon:
        query = query.strip(';') # remove trailing semicolon (also leading, if there's one there for some reason)
    if param_prefix:
        # I could put a list of expected parameter prefix characters in this re but I'm kinda scared it'll affect queries elsewhere
        query = re.sub(r'[&]{1}', param_prefix, query)
    return re.sub(r'\s+', ' ', query).strip()

def get_paths(parent):
    """Produces a list of relative filepaths for all files in any directory below parent.
    Directories are navigated recursively.
    The output of this function is intended to be passed as input to make_dict_from_paths().

    Args:
        parent (path): base directory to start from.

    Returns:
        list[path]: list of paths to all files in directories below parent.
    """
    paths = []
    for root, _, files in os.walk(parent):
        for file in files:
            paths.append(os.path.join(root, file))
    return paths

def make_dict_from_paths(paths):
    """Convert a list of file paths into a nested dictionary of query strings.
    Code adapted from this SO post by user DarrylG: https://stackoverflow.com/a/66995788

    Note: this will include the shared highest level directory, which is undesirable for
    its use in this application. 

    Args:
        paths(list(path)): a list of pathlike objects.

    Returns
        dict(str : str): a dictionary associating file paths to their content.
    """
    # TODO rewrite this method to be more generic or add more type checking and exceptions to make 
    # sure that it doesn't crash if a none-sql file gets in there somehow.

    # Sort so deepest paths are first
    paths = sorted(paths, key = lambda s: len(s.lstrip('\\').split('\\')), reverse = True)

    tree_path = {}
    for path in paths:
        query = strip_query(path, drop_semicolon = True, param_prefix = ':')

        # Split into list and remove leading '/' if present
        levels = path.lstrip('\\').split('\\')
        
         # Get the last element in levels (the filename) and remove the file extension
        file = levels.pop()[:-4]

        # Determine the key that will be used to identify the individual query
        if '_sp_' in file:
            # If the query is a stored procedure call, it gets a more verbose key to distinguish it
            file_key = file[file.find('_sp_') + 4:]
            pass
        else:
            # Otherwise, individual queries will be referred to by the last chunk of their filename
            file_key = file.split('_')[-1]


        acc = tree_path # assign existing tree structure to acc
        for level, path_at_level in enumerate(levels, start = 1):
            # If we reach the last level in levels:
            if level == len(levels):
                # I don't actually know how this works :P
                acc[path_at_level] = acc[path_at_level] if path_at_level in acc else {}
                if isinstance(acc[path_at_level], dict):
                    # Only append if we reach the empty dictionary
                    acc[path_at_level].update({file_key : query})
            else:
                # Create path_at_level as a key with an empty dict as its value
                acc.setdefault(path_at_level, {})
            acc = acc[path_at_level]

    return tree_path

def generate_query_yaml(query_dir, yaml_filename):
    """Generates a .yaml file from a directory (and its subdirectories) of SQL Queries. 
    Provides structured access to query contents in code without having to constantly open
    new file streams. 

    Args:
        query_dir (path): path to top-level directory containing .sql files.
        yaml_filename (path): path including filename where .yaml output should be written to.
    """
    # TODO make yaml_filename optional and have the method return the dict as an object
    # when yaml_filename is not passed, similar to how yaml.dump already works.

    paths = get_paths(query_dir)
    # Specify the 'queries' key to unwrap the highest level directory
    dict = make_dict_from_paths(paths)['queries']
    # Exclude the 'create_database' T-SQL file if it gets included in the dict
    if 'database' in dict:
        del dict['database']
    with open(yaml_filename, 'w') as stream:
        yaml.dump(dict, stream, width = float('inf')) # width = float('inf') ensures that whole queries get dumped to a single line
        head, tail = os.path.split(yaml_filename) # head will be empty if the filename doesn't contain a path
        if not head:
            logging.info(f'Query file {tail} has been written to the active directory')
        else:
            logging.info(f'Query file {tail} has been written to {head}')
