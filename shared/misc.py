import logging

import pyodbc

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
