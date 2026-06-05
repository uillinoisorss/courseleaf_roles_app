"""Functions written specifically for the CourseLeaf Roles App data load process.
"""
from datetime import datetime
import io
import logging
import os
import tempfile
from timeit import default_timer as timer

from dotenv import load_dotenv
import pandas as pd
import paramiko

import shared.query_functions as qf
import shared.etl_functions as etl

######################################################################################################
# CONSTANTS / SETUP
######################################################################################################

load_dotenv()

DENODO_BASE_URL = str(os.getenv('DENODO_BASE_URL'))
DENODO_USERNAME = str(os.getenv('DENODO_USERNAME'))
DENODO_PASSWORD = str(os.getenv('DENODO_PASSWORD'))

REG_HOSTNAME = str(os.getenv('REG_HOST'))
REG_USERNAME = str(os.getenv('REG_USERNAME'))
REG_PASSWORD = str(os.getenv('REG_PASSWORD'))

REPTPROD_HOSTNAME = str(os.getenv('REPTPROD_HOST'))
REPTPROD_USERNAME = str(os.getenv('REPTPROD_USERNAME'))
REPTPROD_PASSWORD = str(os.getenv('REPTPROD_PASSWORD'))

XFERPROD_HOSTNAME = str(os.getenv('XFERPROD_HOST'))
XFERPROD_USERNAME = str(os.getenv('XFERPROD_USERNAME'))
XFERPROD_PKEY = str(os.getenv('XFERPROD_PKEY')).replace('|', '\n')

DB_FILE_NAME = str(os.getenv('DB_FILE_NAME'))

# Tried using os.path.join here, but it didn't play well with the SFTP connector
XFERPROD_DATA_DIRECTORY = str(os.getenv('XFERPROD_DATA_DIRECTORY'))
LOCAL_TEMP_DIRECTORY = tempfile.gettempdir()

SFTP_PATH_TO_DB = XFERPROD_DATA_DIRECTORY + DB_FILE_NAME
LOCAL_PATH_TO_DB = os.path.join(LOCAL_TEMP_DIRECTORY, DB_FILE_NAME)

QUERY_FOLDER_PATH = str(os.getenv('QUERY_FOLDER_PATH')) # All SQL queries in this directory will be loaded into a .yaml file
# QUERY_FILE_PATH = str(os.getenv('QUERY_FILE_PATH')) # A .yaml file containing all queries in QUERY_FOLDER_PATH will be written at this path
QUERY_DB_NAME = str(os.getenv('QUERY_DB_NAME'))
QUERY_SCHEMA_NAME = str(os.getenv('QUERY_SCHEMA_NAME'))


QUERIES = qf.load_queries(QUERY_FOLDER_PATH, database_name = QUERY_DB_NAME, schema_name = QUERY_SCHEMA_NAME)

# Logger setup
logger = logging.getLogger(__name__)
logging.basicConfig(filename = 'dev.log', encoding = 'utf-8', level = logging.DEBUG, format = '%(asctime)s %(levelname)s: %(message)s', datefmt = '%Y-%m-%d %H:%M:%S')

######################################################################################################
# FUNCTIONS
######################################################################################################

def validate_temp_directory():
    """Ensure that local temporary storage directory exists and delete any existing database file.
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
    Copy CourseLeaf application database file from SFTP server to local storage.

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
        logger.info(f'Successfully connected via SSH to {str(XFERPROD_HOSTNAME)}')

        # File transfer
        sftp = ssh.open_sftp()
        sftp.get(SFTP_PATH_TO_DB, LOCAL_PATH_TO_DB)
        logger.info('Database file retrieved via SFTP.')

        # Connection cleanup
        sftp.close()
        ssh.close()
    except Exception as e:
        logger.error(f'An exception was raised while attempting to retrieve database file via SFTP: {str(e)}')
    finally:
        end = timer()
        logger.info(f'Time elapsed during database import: {str(end - start)} seconds.')

    return os.path.exists(LOCAL_PATH_TO_DB)

def get_load_id():
    """Get the current load id number. Data loads in the CourseLeaf database are centrally tracked in the imports tables, 
    which allows all of the data that is loaded at one time to be associated with a single numeric identifier. This query 
    looks at the imports table and finds the maximum existing load_id. This value is then passed to the remaining load functions.

    Returns:
        int: current max load id. If the imports table for that table group is empty, the value 0 will be returned.
    """
    extract_query = QUERIES['reg']['select']['loadid']
    # need to unpack the value that is retrieved by this query
    load_id = etl.extract_from_sql_server(server = REG_HOSTNAME, user = REG_USERNAME, password = REG_PASSWORD, query = extract_query)[0][0]
    return load_id
    
def insert_new_import_record():
    """Insert a row into the imports table, generating a new load_id which is returned.

    Returns:
        int: the load_id associated with this import record. See get_next_load_id for details. Returns -1
            if something goes wrong; 0 if the _imports table is empty; otherwise will return a positive integer.
    """
    insert_query = QUERIES['reg']['insert']['imports']
    load_timestamp = datetime.now()
    params = etl.parameterize_data_frame(pd.DataFrame(data = {'load_timestamp' : [datetime.now()]}))
    etl.insert_to_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, insert_query, parameters = params)
    load_id = get_load_id()
    logger.info(f'An import record with load_id = {load_id} was inserted at {load_timestamp}')
    return load_id

def get_current_terms():
    """Retrieves codes for the current term, previous term, next term, and next next term from the ORMaintenance database.

    Returns:
        tuple[str]: tuple containing four 6-digit term codes as strings.
    """
    extract_query = QUERIES['reg']['select']['terms']['current']
    terms = etl.extract_from_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, extract_query)
    # Query results are a tuple wrapped in a list, so need to access the tuple from the list first.
    return terms[0]

def get_column_names(table_group: str, table: str):
    """Retrieves column names, omitting the mostly-universal metadata columns (id, load_id, insert_timestamp).
    Intended as a convenience to ensure proper ordering of data retrieved via API before insert to SQL Server.

    Args:
        table_group (str): name of the table group ('banner', 'courseleaf')
        table (str): the identifying portion of the table name (what comes after the final _, for example, 'courses', 'departments', 'terms', etc.)

    Returns:
        list[str]: ordered list of column names
    """
    extract_query = QUERIES['reg']['select']['columns']
    table_name = f'{table_group.lower()}_{table.lower()}'
    column_names = etl.extract_from_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, extract_query, parameters = [table_name])
    # Single-element rows still get returned as tuples, so need to unpack them first
    column_names = [row[0] for row in column_names]
    return column_names
    
# BANNER ETL FUNCTIONS

# TODO docstring
def extract_from_banner(table: str, params: dict = {}):
    table_group = 'banner'
    endpoint = f'/views/{table_group.lower()}_{table.lower()}'
    url = DENODO_BASE_URL + endpoint
    process_name = f'{table_group.upper()}_{table.upper()}'
    try:
        json = etl.get_api_data(url, DENODO_USERNAME, DENODO_PASSWORD, params)
        data = etl.json_to_dataframe(json)
        if params:
            display_params = ';'.join([f'{key} : {params[key]}' for key in params])
            logger.info(f'{process_name}: Read {data.shape[0]} rows from REPTPROD using parameters: {display_params}')
        else:
            logger.info(f'{process_name}: Read {data.shape[0]} rows from REPTPROD.')
    except Exception as e:
        raise
    return data

# TODO docstring
def load_banner_data(data: pd.DataFrame, table: str):
    table_group = 'banner'
    process_name = f'{table_group.upper()}_{table.upper()}'
    column_names = get_column_names(table_group, table)
    load_id = get_load_id()
    load_query = QUERIES['reg']['insert'][table_group][table]
    preprocessed_data = etl.preprocess_dataframe(data, load_id, column_names)
    logger.info(f'{process_name}: Prepared {preprocessed_data.shape[0]} rows for load to SQL Server.')
    load_data = etl.parameterize_data_frame(preprocessed_data)
    try:
        etl.insert_to_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, load_query, load_data)
        logger.info(f'{process_name}: Load to SQL Server complete.')
    except Exception as e:
        raise

def execute_banner_data_load():
    """Triggers all Banner ETL functions in sequence.
    """
    tables = ['courses', 'departments', 'subjects', 'terms', 'userinfo']
    for table in tables:
        # banner_courses table gets special treatment, both because the data has to be pulled for each term
        # and because course data needs an extra processing step
        if table == 'courses':
            terms = get_current_terms()
            for term in terms:
                params = {'course_term' : term}
                data = extract_from_banner(table, params)
                # The term parameter for the Denodo view had to be something other than just 'term' since I'd already
                # used that in creating the base views, so instead it's called 'course_term'. I change it back to
                # just 'term' here for compatability with the SQL Server table.
                data.rename(columns = {'course_term' : 'term'}, inplace = True)
                load_banner_data(data, table)
        # banner_userinfo also needs special handling to avoid loading the contact info for every single individual
        # in Banner every night.
        elif table == 'userinfo':
            extract_query = QUERIES['reg']['select']['courseleaf']['users']
            user_uins = etl.extract_from_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, extract_query)
            user_uins = [row[0] for row in user_uins]
            data = extract_from_banner(table)
            # Filter data to only include UINs present in the courseleaf_users table
            # TODO might be wise to log UINs that don't have email addresses somewhere
            data = data[data['uin'].isin(user_uins)]
            load_banner_data(data, table)
        else:
            data = extract_from_banner(table)
            load_banner_data(data, table)

# COURSELEAF ETL FUNCTIONS

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
        raise
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
    load_id = get_load_id()
    roles['load_id'] = load_id
    roles['insert_timestamp'] = datetime.now()
    roles = roles[['load_id', 'role', 'dept_no', 'dept', 'role_title', 'uin', 'insert_timestamp']]
    logger.info(f'COURSELEAF_ROLES: Prepared {roles.shape[0]} rows for load to SQL Server.')

    # LOAD DATA TO SQL SERVER

    load_query = QUERIES['reg']['insert']['courseleaf']['roles']
    load_data = etl.parameterize_data_frame(roles)
    try:
        etl.insert_to_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, load_query, load_data)
        logger.info(f'COURSELEAF_ROLES: Load to SQL Server complete.')
    except Exception as e:
        logger.error(str(e))
    sp_query = QUERIES['reg']['stored_procedures']['update_current_roles']
    qf.run_sql_server_query(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, sp_query)

def extract_and_load_courseleaf_users():
    """Retrieve user identity data from CourseLeaf database file, transform results, and load to SQL Server.
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
    load_id = get_load_id()
    users['load_id'] = load_id
    users['insert_timestamp'] = datetime.now()
    users = users[['load_id'] + columns + ['insert_timestamp']]
    logger.info(f'COURSELEAF_USERS: Prepared {users.shape[0]} rows for load to SQL Server.')

    load_query = QUERIES['reg']['insert']['courseleaf']['users']
    load_data = etl.parameterize_data_frame(users)
    try:
        etl.insert_to_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, load_query, load_data)
        logger.info(f'COURSELEAF_USERS: Load to SQL Server complete.')
    except Exception as e:
        logger.error(f'COURSELEAF_USERS: {str(e)}')

def execute_courseleaf_data_load():
    """Triggers all CourseLeaf ETL functions in sequence.
    """
    extract_and_load_courseleaf_roles()
    extract_and_load_courseleaf_users()

# AGGREGATE FUNCTIONS

def update_current_tables():
    """Executes stored procedures that copy data from load tables to current tables.
    """
    stored_procedures = ['get_import_rowcounts', 'update_current_courses', 'update_current_crosslists',
                         'update_current_departments', 'update_current_roles', 'update_current_subjects', 
                         'update_current_terms', 'update_current_users', 'generate_powerbi_crosslists', 
                         'generate_powerbi_roles', 'update_ormaintenance_terms']
    for proc_name in stored_procedures:
        sp_query = QUERIES['reg']['stored_procedures'][proc_name]
        qf.run_sql_server_query(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, sp_query)

def truncate_database_tables():
    """Truncates all application database tables. Only to be used when resetting database for testing purposes.
    """
    # Banner
    for table in ['courses', 'departments', 'subjects', 'terms', 'userinfo']:
        truncate_query = QUERIES['reg']['truncate']['banner'][table]
        qf.run_sql_server_query(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, truncate_query)
    # Courseleaf
    for table in ['roles', 'users']:
        truncate_query = QUERIES['reg']['truncate']['courseleaf'][table]
        qf.run_sql_server_query(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, truncate_query)
    # Current
    for table in ['courses', 'crosslists', 'departments', 'imports', 'roles', 'subjects', 'terms', 'users']:
        truncate_query = QUERIES['reg']['truncate']['current'][table]
        qf.run_sql_server_query(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, truncate_query)
    # Power BI
    for table in ['crosslists']:
        truncate_query = QUERIES['reg']['truncate']['powerbi'][table]
        qf.run_sql_server_query(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, truncate_query)

def execute_data_load():
    """Executes the entire CourseLeaf application data load process.
    """
    logger.info('COURSELEAF_CONTACTS: Initializing data load.')

    # Bring in SQLite file from LeepFrog and confirm that it's accessible
    import_complete = import_database_file()

    # All of the ETL stuff happens here
    if import_complete:
        try:
            insert_new_import_record()
            execute_courseleaf_data_load() # CourseLeaf data needs to be loaded first so that the user list is current for the Banner data load
            execute_banner_data_load()
            update_current_tables()
            logger.info('COURSELEAF_CONTACTS: Data load complete.')
        except Exception as e:
            logger.error(f'COURSELEAF_CONTACTS: An error occurred during the data load process: {e}')
    else:
        logger.error('COURSELEAF_CONTACTS: The CourseLeaf database file was not found. Aborting data load.')
