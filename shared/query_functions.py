import logging
import os
import re

import pyodbc
import yaml

# Maybe I can rename this file to query_functions?

# Run a SQL Server query without returning anything.
# Primarily intended for running truncate queries and the like.
def run_sql_server_query(server, user, password, query, parameters = None):
    connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};UID={user};PWD={password};TrustServerCertificate=yes;'
    try:
        with pyodbc.connect(connection_string, trusted_connection = 'Yes') as connection:
            with connection.cursor() as cursor:
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                logging.info(f'Executed query: {query} with parameters {parameters}')
    except Exception as e:
        logging.error(f'An exception was raised while connecting to SQL Server {server}: {str(e)}')
        raise

# Functions for creating SQL query YAML file

def strip_query(path, drop_semicolon = False, param_prefix = None):
    """
    """
    with open(path) as file:
        lines = file.readlines()
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
    """Convert a list of file paths into a nested dictionary.
    Code adapted from this SO post by user DarrylG: https://stackoverflow.com/a/66995788

    Note: this will include the shared highest level directory, which is undesirable for
    its use in this application. 
    """
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
    paths = get_paths(query_dir)
    # Specify the 'queries' key to unwrap the highest level directory
    dict = make_dict_from_paths(paths)['queries']
    # Exclude the 'create_database' T-SQL file if it gets included in the dict
    if 'database' in dict:
        del dict['database']
    with open(yaml_filename, 'w') as stream:
        yaml.dump(dict, stream, width = float('inf')) # width = float('inf') ensures that whole queries get dumped to a single line