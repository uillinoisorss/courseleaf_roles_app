from datetime import datetime
import io
import logging
import os
import sqlite3
import sys
import tempfile
from timeit import default_timer as timer
import yaml

import azure.functions as func
from dotenv import load_dotenv
import pandas as pd
import paramiko
import pyodbc

######################################################################################################
# DECLARING ENVIRONMENT VARIABLES 
######################################################################################################

load_dotenv()

SFTP_HOSTNAME = str(os.getenv('XFERPROD_HOST'))
SFTP_USERNAME = str(os.getenv('XFERPROD_USERNAME'))
SFTP_PKEY = str(os.getenv('XFERPROD_KEY'))

SQL_SERVER_HOSTNAME = str(os.getenv('REG_HOST'))
SQL_SERVER_USERNAME = str(os.getenv('REG_USERNAME'))
SQL_SERVER_PASSWORD = str(os.getenv('REG_PASSWORD'))
SQL_SERVER_CONNECTION_STRING = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_SERVER_HOSTNAME};UID={SQL_SERVER_USERNAME};PWD={SQL_SERVER_PASSWORD};TrustServerCertificate=yes;'

DB_FILE_NAME = 'tcfdb.sqlite'

# Tried using os.path.join here, but it didn't play well with the SFTP connector
SFTP_DATA_DIRECTORY = '/export/home/student/scat/CourseLeafDataWarehouse/'
LOCAL_TEMP_DIRECTORY = tempfile.gettempdir()

SFTP_PATH_TO_DB = SFTP_DATA_DIRECTORY + DB_FILE_NAME
LOCAL_PATH_TO_DB = os.path.join(LOCAL_TEMP_DIRECTORY, DB_FILE_NAME)

######################################################################################################
# SQL QUERIES 
######################################################################################################

with open('queries.yaml') as query_file:
        QUERIES = yaml.load(query_file)

######################################################################################################
# FUNCTIONS
######################################################################################################

def validate_temp_directory():
    """
    Ensure that local temporary storage directory exists and delete any existing database file.
    """
    if not os.path.exists(LOCAL_TEMP_DIRECTORY):
        try:
            logging.info('DEBUG: Creating temporary local directory at: ' + str(LOCAL_TEMP_DIRECTORY))
            os.makedirs(LOCAL_TEMP_DIRECTORY)
        except Exception as e:
            logging.error('An exception occurred while creating temporary local directory: ' + str(e))

    if os.path.exists(LOCAL_PATH_TO_DB):
        logging.info('DEBUG: Deleting existing database file at: ' + str(LOCAL_PATH_TO_DB))
        os.remove(LOCAL_PATH_TO_DB)

def import_database_file() -> bool:
    """
    Copy database file from SFTP server to local storage.

    Returns:
        bool: True if the database file exists in local storage after import, False otherwise.
    """
    try:
        start = timer()

        # Create SFTP connection
        # (paramiko does not support context managers, which is why I explicitly create and close the connections here)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        key = paramiko.RSAKey.from_private_key(io.StringIO(SFTP_PKEY))
        ssh.connect(hostname = SFTP_HOSTNAME, port = 22, username = SFTP_USERNAME, pkey = key)
        logging.info('DEBUG: Successfully connected via SSH to ' + str(SFTP_HOSTNAME))

        # File transfer
        sftp = ssh.open_sftp()
        sftp.get(SFTP_PATH_TO_DB, LOCAL_PATH_TO_DB)
        logging.info('DEBUG: Successfully retrieved database file via SFTP.')

        # Connection cleanup
        sftp.close()
        ssh.close()
    except Exception as e:
        # More detailed error information I probably don't need:
        # type, value, traceback = sys.exc_info()
        # logging.error(f'{type}, {value}, {traceback}')
        logging.error('An exception was raised while attempting to retrieve database file via SFTP: ' + str(e))
    finally:
        end = timer()
        logging.info('DEBUG: Time elapsed during database import: ' + str(end - start) + ' seconds.')

    return os.path.exists(LOCAL_PATH_TO_DB)

def get_role_load_id() -> int:
    """
    Determines the appropriate load id number for next role data load.
    Returns -1 if connection fails for any reason.

    Returns:
        int: new load id that is one greater than last load id
    """
    registrar_select_load_id_query = QUERIES['registrar']['select']['load_id']

    # TODO delete this after confirming that the YAML storage is the right way to go
    # load_id_query = """
    # SELECT
    #     ISNULL(MAX(load_id), 0)
    # FROM
    #     [CourseLeaf_Contacts].[dbo].[courseleaf_role_data_load]
    # """
    
    try:
        with pyodbc.connect(SQL_SERVER_CONNECTION_STRING, trusted_connection = 'Yes') as connection:
            logging.info('DEBUG: Successfully connected to ' + str(SQL_SERVER_HOSTNAME))
            with connection.cursor() as cursor:
                cursor.execute(registrar_select_load_id_query)
                previous_load_id = cursor.fetchone()
        new_load_id = previous_load_id[0] + 1
        logging.info('DEBUG: New ID for role data load is: ' + str(new_load_id))
        return new_load_id
    except Exception as e:
        logging.error('An exception was raised while connecting to ' + str(SQL_SERVER_HOSTNAME) + ': ' + str(e))
        return -1
    
def extract_role_data_from_db_file() -> pd.DataFrame:
    """
    Retrieves role membership data from SQLite database and transforms it in preparation
    for loading to SQL server.

    Returns:
        pd.DataFrame: A dataframe with the following columns (all fields are of type str):
            - role
            - dept_no
            - dept
            - role_title
            - uin
            - last_name
            - first_name
            - email
    """
    courseleaf_select_roles_query = QUERIES['courseleaf']['select']['roles']
    # Query to display all CourseLeaf role information (this query was provided by Leepfrog)
    # role_query = """
    # SELECT 
    #     s1.value as name, s2.value as members, s3.value as email
    # FROM 
    #     pages
    #     JOIN tcdata using(pagekey)
    #     JOIN tcval s1 using(tckey)
    #     JOIN tcval s2 using (tckey,rank)
    #     JOIN tcval s3 using (tckey,rank)
    # WHERE 
    #     pages.path = '/courseleaf/roles.html' AND tcdata.tctype = 'tcf'  
    #     AND s1.part= 'name' AND s2.part= 'members' AND s3.part = 'email'
    # """

    # Query to pull in role member information (name & email as listed in CourseLeaf)
    courseleaf_select_users_query = QUERIES['courseleaf']['select']['users']
    # courseleaf_select_users_query = """
    # SELECT userid, lname, fname, email FROM users
    # """

    connection = sqlite3.connect(LOCAL_PATH_TO_DB)
    cursor = connection.cursor()
    
    cursor.execute(courseleaf_select_roles_query)
    roles = pd.DataFrame.from_records(cursor.fetchall(), columns = ['role', 'members', 'email'])

    cursor.execute(courseleaf_select_users_query)
    role_member_info = pd.DataFrame.from_records(cursor.fetchall(), columns = ['uin', 'last_name', 'first_name', 'email'])

    cursor.close()
    connection.close()

    logging.info('DEBUG: Successfully queried local database file.')

    # The roles we're interested are formatted like this: 0000-DEPT Role
    # To filter out other roles (mostly College level roles), we look only
    # for roles that contain a hyphen (-) and start with a numeric chatacter.
    roles = roles[roles['role'].str.contains('-')]
    roles = roles[roles['role'].str.contains('[0-9]+')]

    # To split the role titles into usable chunks, we first split on the hyphen, extracting the department number:
    roles[['dept_no', 'role_title']] = roles['role'].astype(str).str.split('-', expand = True)

    # We now split on the first space in what's left to separate the department name from the role title. 
    # We only look at the first space because role titles can contain multiple spaces.
    roles[['dept', 'role_title']] = roles['role_title'].astype(str).str.split(' ', n = 1, expand = True)
    roles = roles[['role', 'dept_no', 'dept', 'role_title', 'members', 'email']]
    roles.set_index('role', inplace = True)

    # Role members are given by UIN in a single, comma-delimited data element, so we need to break those up. 
    # This chunk gets the UINs alongside the corresponding role title in tidy row format.
    members = roles['members'].str.split(',', expand = True)
    members = members.stack()
    members.rename("uin", inplace = True)
    members = members.to_frame()

    # Drop excess data from roles table before joining.
    roles = roles[['dept_no', 'dept', 'role_title']]
        
    # Join role title/department information with UIN data, then set UIN
    # as the index since that's now the primary key value in our dataset.
    roles = roles.join(members)
    roles.reset_index(inplace = True)
    roles = roles[['uin', 'role', 'dept_no', 'dept', 'role_title' ]]
    roles.set_index('uin', inplace = True)

    role_member_info.set_index('uin', inplace = True)

    # Join role info to role member info--this is the dataset that will be loaded to 
    # the REG database & referred back to by the CourseLeaf contacts tool.
    role_data = roles.join(role_member_info)
    role_data.reset_index(inplace = True)

    return role_data

def load_role_data_to_sql_server(role_data: pd.DataFrame, load_id: int, load_timestamp: datetime):
    """
    Takes data retrieved by extract_role_data_from_db_file() and inserts it in SQL server database.

    Parameters:
        role_data (pd.DataFrame): DataFrame created by extract_role_data_from_db_file()
        load_id (int): Identifier for this data load, retrieved by get_role_load_id()
        load_timestamp (datetime.datetime): Datetime representing when data is loaded to SQL server.
    """
    load_data = role_data.copy()
    load_data['load_id'] = load_id
    load_data['load_timestamp'] = load_timestamp

    load_data = load_data[['load_id', 'role', 'dept_no', 'dept', 'role_title', 'uin', 'last_name', 'first_name', 'email', 'load_timestamp']]

    insert_data = [tuple(item) for item in load_data.itertuples(index = False)]

    insert_query = """
    INSERT INTO
        [CourseLeaf_Contacts].[dbo].[courseleaf_role_data_load]
        (load_id,
        role,
        dept_no,
        dept,
        role_title,
        uin,
        last_name,
        first_name,
        email,
        load_timestamp)
    VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        with pyodbc.connect(SQL_SERVER_CONNECTION_STRING, trusted_connection = 'Yes') as connection:
            with connection.cursor() as cursor:
                cursor.executemany(insert_query, insert_data)
        logging.info('DEBUG: Insert to SQL Server instance completed successfully.')
    except Exception as e:
        logging.error('An exception was raised while connecting to ' + str(SQL_SERVER_HOSTNAME) + ': ' + str(e))

def test_sql_server_connection():
    try:
        with pyodbc.connect(SQL_SERVER_CONNECTION_STRING, trusted_connection = 'Yes') as connection:
            with connection.cursor() as cursor:
                sql = """SELECT TOP 1 * FROM [CourseLeaf_Contacts].[dbo].[courseleaf_role_members]"""
                cursor.execute(sql)
                output = cursor.fetchall()
        logging.info('SQL server query successful.')
        return func.HttpResponse("Successfully connected to SQL server database. Data: " + str(output))
    except Exception as e:
        logging.error('Connection failed: ' + str(e))
        return func.HttpResponse(str(e))


######################################################################################################
# APP CODE
######################################################################################################

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="courseleaf_roles_app_data_load")
def courseleaf_roles_app_data_load(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )