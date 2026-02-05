"""Complete process for loading CourseLeaf application data to SQL Server database without Azure.
"""
import io
import logging
import os
import sys
import tempfile
from timeit import default_timer as timer

from dotenv import load_dotenv
import paramiko
from yaml import safe_load as load

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
            logger.debug(f'Creating temporary local directory at: {str(LOCAL_TEMP_DIRECTORY)}')
            os.makedirs(LOCAL_TEMP_DIRECTORY)
        except Exception as e:
            logger.error(f'An exception occurred while creating temporary local directory: {str(e)}')

    if os.path.exists(LOCAL_PATH_TO_DB):
        logger.debug(f'Deleting existing database file at: {str(LOCAL_PATH_TO_DB)}')
        os.remove(LOCAL_PATH_TO_DB)

    logger.info('Temp directory validated.')

def import_database_file() -> bool:
    """
    Copy database file from SFTP server to local storage.

    Returns:
        bool: True if the database file exists in local storage after import, False otherwise.
    """
    start = timer()
    try:
        # Create SFTP connection
        # (paramiko does not support context managers, which is why I explicitly create and close the connections here)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        key = paramiko.RSAKey.from_private_key(io.StringIO(XFERPROD_PKEY))
        ssh.connect(hostname = XFERPROD_HOSTNAME, port = 22, username = XFERPROD_USERNAME, pkey = key)
        logger.debug(f'Successfully connected via SSH to {str(XFERPROD_HOSTNAME)}')

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
        return new_load_id
    except Exception as e:
        logging.error(f'An exception was raised while connecting to {str(REG_HOSTNAME)}: {str(e)}')
        return -1

def load_courseleaf_courses():
    pass

def load_courseleaf_departments():
    pass

def extract_and_load_courseleaf_roles():
    extract_query = QUERIES['courseleaf']['select']['roles']
    load_query = QUERIES['registrar']['insert']['roles']

def load_courseleaf_subjects():
    pass

def load_courseleaf_users():
    pass

def load_courseleaf_data():
    pass

######################################################################################################
# CODE EXECUTION
######################################################################################################

def main():
    print(get_next_load_id('roles'))

if __name__ == "__main__":
    main()