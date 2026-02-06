"""Complete process for loading CourseLeaf application data to SQL Server database without Azure.
"""
from datetime import datetime
import io
import logging
import os
import sys
import tempfile
from timeit import default_timer as timer

from dotenv import load_dotenv
import pandas as pd
import paramiko
from yaml import safe_load as load

import shared.misc as misc
import shared.etl_functions as etl

######################################################################################################
# ENVIRONMENT VARIABLES / SETUP
######################################################################################################

load_dotenv()

REG_HOSTNAME = str(os.getenv('REG_HOST'))
REG_USERNAME = str(os.getenv('REG_USERNAME'))
REG_PASSWORD = str(os.getenv('REG_PASSWORD'))

REPTPROD_HOSTNAME = str(os.getenv('REPTPROD_HOST'))
REPTPROD_USERNAME = str(os.getenv('REPTPROD_HOST'))
REPTPROD_PASSWORD = str(os.getenv('REPTPROD_HOST'))

XFERPROD_HOSTNAME = str(os.getenv('XFERPROD_HOST'))
XFERPROD_USERNAME = str(os.getenv('XFERPROD_USERNAME'))
XFERPROD_PKEY = str(os.getenv('XFERPROD_PKEY'))

DB_FILE_NAME = 'tcfdb.sqlite'

# Tried using os.path.join here, but it didn't play well with the SFTP connector
XFERPROD_DATA_DIRECTORY = '/export/home/student/scat/CourseLeafDataWarehouse/'
LOCAL_TEMP_DIRECTORY = tempfile.gettempdir()

SFTP_PATH_TO_DB = XFERPROD_DATA_DIRECTORY + DB_FILE_NAME
LOCAL_PATH_TO_DB = os.path.join(LOCAL_TEMP_DIRECTORY, DB_FILE_NAME)

# Logger setup
logger = logging.getLogger(__name__)
logging.basicConfig(filename = 'dev.log', encoding = 'utf-8', level = logging.DEBUG, format = '%(asctime)s %(levelname)s: %(message)s', datefmt = '%Y-%m-%d %H:%M:%S')

######################################################################################################
# SQL QUERIES 
######################################################################################################

with open('queries.yaml') as query_file:
        QUERIES = load(query_file)

######################################################################################################
# FUNCTIONS
######################################################################################################

def validate_temp_directory():
    """
    Ensure that local temporary storage directory exists and delete any existing database file.
    """
    if not os.path.exists(LOCAL_TEMP_DIRECTORY):
        try:
            logger.info(f'Creating temporary local directory at: {str(LOCAL_TEMP_DIRECTORY)}')
            os.makedirs(LOCAL_TEMP_DIRECTORY)
        except Exception as e:
            logger.error(f'An exception occurred while creating temporary local directory: {str(e)}')

    if os.path.exists(LOCAL_PATH_TO_DB):
        logger.info(f'Deleting existing database file at: {str(LOCAL_PATH_TO_DB)}')
        os.remove(LOCAL_PATH_TO_DB)

    logger.info('Temp directory validated.')

def import_database_file() -> bool:
    """
    Copy database file from SFTP server to local storage.

    Returns:
        bool: True if the database file exists in local storage after import, False otherwise.
    """
    # TODO delete this after debugging, this clause is just to prevent having to re-import the database file
    # every time I run the process while debugging.
    if os.path.exists(LOCAL_PATH_TO_DB):
        return True

    start = timer()
    try:
        # Create SFTP connection
        # (paramiko does not support context managers, which is why I explicitly create and close the connections here)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        key = paramiko.RSAKey.from_private_key(io.StringIO(XFERPROD_PKEY))
        ssh.connect(hostname = XFERPROD_HOSTNAME, port = 22, username = XFERPROD_USERNAME, pkey = key)
        logger.info(f'Successfully connected via SSH to {str(XFERPROD_HOSTNAME)}')

        # File transfer
        sftp = ssh.open_sftp()
        sftp.get(SFTP_PATH_TO_DB, LOCAL_PATH_TO_DB)
        logger.info('Database file retrieved via SFTP.')

        # Connection cleanup
        sftp.close()
        ssh.close()
    except Exception as e:
        # More detailed error information I probably don't need:
        # type, value, traceback = sys.exc_info()
        # logger.error(f'{type}, {value}, {traceback}')
        logger.error(f'An exception was raised while attempting to retrieve database file via SFTP: {str(e)}')
    finally:
        end = timer()
        logger.info(f'Time elapsed during database import: {str(end - start)} seconds.')

    return os.path.exists(LOCAL_PATH_TO_DB)

def get_next_load_id(table):
    extract_query = QUERIES['registrar']['select']['load_id'][table]
    try:
        prev_load_id = etl.extract_from_sql_server(server = REG_HOSTNAME, user = REG_USERNAME, password = REG_PASSWORD, query = extract_query)
        # Get the first (only) row returned by the cursor, then get the max_load_id column from that row.
        new_load_id = prev_load_id[0].max_load_id + 1
        logger.info(f'Next load_id for {table} is {new_load_id}.')
        return new_load_id
    except Exception as e:
        logger.error(f'An exception was raised while connecting to {str(REG_HOSTNAME)}: {str(e)}')
        return -1

def extract_and_load_courseleaf_courses():
    """
    """
    extract_query = QUERIES['courseleaf']['select']['courses']
    try:
        extract_results = etl.extract_from_sqlite(LOCAL_PATH_TO_DB, extract_query)
    except Exception as e:
        logger.error(f'COURSELEAF_COURSES: {str(e)}')
    columns = ['course', 'subject_code', 'course_no', 'course_title']
    courses = etl.query_results_to_dataframe(extract_results, columns)
    logger.info(f'COURSELEAF_COURSES: Read {courses.shape[0]} rows from SQLite database.')

    courses.fillna('', inplace = True)
    courses['load_id'] = get_next_load_id('courses')
    courses['insert_timestamp'] = datetime.now()
    courses = courses[['load_id'] + columns + ['insert_timestamp']]
    logger.info(f'COURSELEAF_COURSES: Prepared {courses.shape[0]} rows for load to SQL Server.')

    load_query = QUERIES['registrar']['insert']['courses']
    load_data = etl.parameterize_data_frame(courses)
    try:
        etl.insert_to_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, load_query, load_data)
        logger.info(f'COURSELEAF_COURSES: Load to SQL Server complete.')
    except Exception as e:
        logger.error(f'COURSELEAF_COURSES: {str(e)}')

def extract_and_load_courseleaf_departments():
    """
    """
    extract_query = QUERIES['courseleaf']['select']['departments']
    try:
        extract_results = etl.extract_from_sqlite(LOCAL_PATH_TO_DB, extract_query)
    except Exception as e:
        logger.error(f'COURSELEAF_DEPARTMENTS: {str(e)}')
    columns = ['dept_no', 'dept_name', 'college', 'college_name']
    departments = etl.query_results_to_dataframe(extract_results, columns)
    logger.info(f'COURSELEAF_DEPARTMENTS: Read {departments.shape[0]} rows from SQLite database.')

    departments.fillna('', inplace = True)
    departments['load_id'] = get_next_load_id('departments')
    departments['insert_timestamp'] = datetime.now()
    departments = departments[['load_id'] + columns + ['insert_timestamp']]
    logger.info(f'COURSELEAF_DEPARTMENTS: Prepared {departments.shape[0]} rows for load to SQL Server.')

    load_query = QUERIES['registrar']['insert']['departments']
    load_data = etl.parameterize_data_frame(departments)
    try:
        etl.insert_to_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, load_query, load_data)
        logger.info(f'COURSELEAF_DEPARTMENTS: Load to SQL Server complete.')
    except Exception as e:
        logger.error(f'COURSELEAF_DEPARTMENTS: {str(e)}')

def extract_and_load_courseleaf_roles():
    """Retrieve role membership data from CourseLeaf database file, transform results, and load to SQL Server.

    Because I'm using a query provided by Leepfrog to get the role membership information (because it's structured
    in a kinda bizarre way, but who I am to judge) there are more transformation steps in this part of the data load
    than there are for the other tables.
    """
    # EXTRACT ROLE DATA FROM SQLITE DATABASE
    
    extract_query = QUERIES['courseleaf']['select']['roles']
    try:
        extract_results = etl.extract_from_sqlite(LOCAL_PATH_TO_DB, extract_query)
    except Exception as e:
        logger.error(str(e))
    roles = etl.query_results_to_dataframe(extract_results, ['role', 'members', 'email'])
    logger.info(f'COURSELEAF_ROLES: Read {roles.shape[0]} rows from SQLite database.')

    # TRANSFORM ROLE DATA TO PREPARE FOR LOAD TO SQL SERVER

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
    members.dropna(subset = ['uin'], inplace = True) # stack creates extra empty rows that need to be dropped

    # Drop excess data from roles table before joining.
    roles = roles[['dept_no', 'dept', 'role_title']]
        
    # Join role title/department information with UIN data, then set UIN
    # as the index since that's now the primary key value in our dataset.
    roles = roles.join(members)
    roles.reset_index(inplace = True)
    roles['load_id'] = get_next_load_id('roles')
    roles['insert_timestamp'] = datetime.now()
    roles = roles[['load_id', 'role', 'dept_no', 'dept', 'role_title', 'uin', 'insert_timestamp']]
    logger.info(f'COURSELEAF_ROLES: Prepared {roles.shape[0]} rows for load to SQL Server.')

    # LOAD DATA TO SQL SERVER

    load_query = QUERIES['registrar']['insert']['roles']
    load_data = etl.parameterize_data_frame(roles)
    try:
        etl.insert_to_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, load_query, load_data)
        logger.info(f'COURSELEAF_ROLES: Load to SQL Server complete.')
    except Exception as e:
        logger.error(str(e))

def extract_and_load_courseleaf_subjects():
    """
    """
    extract_query = QUERIES['courseleaf']['select']['subjects']
    try:
        extract_results = etl.extract_from_sqlite(LOCAL_PATH_TO_DB, extract_query)
    except Exception as e:
        logger.error(f'COURSELEAF_SUBJECTS: {str(e)}')
    columns = ['subject_code', 'subject', 'dept_no']
    subjects = etl.query_results_to_dataframe(extract_results, columns)
    logger.info(f'COURSELEAF_SUBJECTS: Read {subjects.shape[0]} rows from SQLite database.')

    subjects.fillna('', inplace = True)
    subjects['load_id'] = get_next_load_id('subjects')
    subjects['insert_timestamp'] = datetime.now()
    subjects = subjects[['load_id'] + columns + ['insert_timestamp']]
    logger.info(f'COURSELEAF_SUBJECTS: Prepared {subjects.shape[0]} rows for load to SQL Server.')

    load_query = QUERIES['registrar']['insert']['subjects']
    load_data = etl.parameterize_data_frame(subjects)
    try:
        etl.insert_to_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, load_query, load_data)
        logger.info(f'COURSELEAF_SUBJECTS: Load to SQL Server complete.')
    except Exception as e:
        logger.error(f'COURSELEAF_SUBJECTS: {str(e)}')

def extract_and_load_courseleaf_users():
    """
    """
    extract_query = QUERIES['courseleaf']['select']['users']
    try:
        extract_results = etl.extract_from_sqlite(LOCAL_PATH_TO_DB, extract_query)
    except Exception as e:
        logger.error(f'COURSELEAF_USERS: {str(e)}')
    columns = ['uin', 'last_name', 'first_name', 'email']
    users = etl.query_results_to_dataframe(extract_results, columns)
    logger.info(f'COURSELEAF_USERS: Read {users.shape[0]} rows from SQLite database.')

    users.fillna('', inplace = True)
    users['load_id'] = get_next_load_id('users')
    users['insert_timestamp'] = datetime.now()
    users = users[['load_id'] + columns + ['insert_timestamp']]
    logger.info(f'COURSELEAF_USERS: Prepared {users.shape[0]} rows for load to SQL Server.')

    load_query = QUERIES['registrar']['insert']['users']
    load_data = etl.parameterize_data_frame(users)
    try:
        etl.insert_to_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, load_query, load_data)
        logger.info(f'COURSELEAF_USERS: Load to SQL Server complete.')
    except Exception as e:
        logger.error(f'COURSELEAF_USERS: {str(e)}')

def execute_courseleaf_data_load():
    """ This just runs all of the other CourseLeaf ETL functions. Is this modularity?
    """
    # Need to manually truncate certain current_ tables that don't yet account for historical
    # data in their triggers. I tried just doing a truncate in each trigger, but turns out that
    # that truncates after every ROW in the current implementation. This isn't a long term solution
    # but it's the best I can do right now.
    for table in ['crosslists', 'departments', 'subjects', 'users']:
        truncate_query = QUERIES['registrar']['truncate']['current'][table]
        misc.run_sql_server_query(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, truncate_query)

    extract_and_load_courseleaf_courses()
    extract_and_load_courseleaf_departments()
    extract_and_load_courseleaf_roles()
    extract_and_load_courseleaf_subjects()
    extract_and_load_courseleaf_users()

######################################################################################################
# CODE EXECUTION
######################################################################################################

def main():
    import_complete = import_database_file()
    if import_complete:
        execute_courseleaf_data_load()

if __name__ == "__main__":
    main()