"""Complete process for loading CourseLeaf application data to SQL Server database without Azure.
"""
from collections import namedtuple
from datetime import datetime
import io
import logging
import os
import tempfile
from timeit import default_timer as timer

from dotenv import load_dotenv
import pandas as pd
import paramiko
from yaml import safe_load as load

import shared.query_functions as qf
import shared.etl_functions as etl

######################################################################################################
# ENVIRONMENT VARIABLES / SETUP
######################################################################################################

load_dotenv()

REG_HOSTNAME = str(os.getenv('REG_HOST'))
REG_USERNAME = str(os.getenv('REG_USERNAME'))
REG_PASSWORD = str(os.getenv('REG_PASSWORD'))

REPTPROD_HOSTNAME = str(os.getenv('REPTPROD_HOST'))
REPTPROD_USERNAME = str(os.getenv('REPTPROD_USERNAME'))
REPTPROD_PASSWORD = str(os.getenv('REPTPROD_PASSWORD'))

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

QUERY_FILE_PATH = 'queries.yaml'

if not os.path.exists(QUERY_FILE_PATH):
    qf.generate_query_yaml('queries', QUERY_FILE_PATH)

with open(QUERY_FILE_PATH) as query_file:
        QUERIES = load(query_file)

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
        logger.error(f'An exception was raised while attempting to retrieve database file via SFTP: {str(e)}')
    finally:
        end = timer()
        logger.info(f'Time elapsed during database import: {str(end - start)} seconds.')

    return os.path.exists(LOCAL_PATH_TO_DB)

def get_load_id(table_group):
    """Get the current load id number for the specified table group. Data loads in the CourseLeaf database 
    are centrally tracked in the _imports tables, which allows all of the data that is loaded at one time 
    to be associated with a single numeric identifier. This query looks at the _imports table for specified 
    table group and finds the maximum existing load_id. This value is then passed to the remaining load functions.

    Args:
        table_group (str): the name of the table group for which to retrieve the load id. Right now,
            the valid table groups are "banner" and "courseleaf".

    Returns:
        int: current load id for specified table group. If the _imports table for that table group is empty, 
        the value 0 will be returned. If there is an error while querying the database, -1 will be returned.
    """
    extract_query = QUERIES['reg']['select']['loadid'][table_group]
    try:
        # need to unpack the value that is retrieved by this query
        load_id = etl.extract_from_sql_server(server = REG_HOSTNAME, user = REG_USERNAME, password = REG_PASSWORD, query = extract_query)[0][0]
        logger.info(f'Next load_id for {table_group} tables is {load_id}.')
        return load_id
    except Exception as e:
        logger.error(f'An exception was raised while connecting to {str(REG_HOSTNAME)}: {str(e)}')
        return -1
    
def insert_new_import_record(table_group):
    """Insert a row into the _imports table for a table group, generating a new load_id which is returned
    by the function.

    I forgot that load_id is identity in the _imports table, lmao

    Args:
        table_group (str): the name of the table group for which to insert an import record. 
        Right now, the valid table groups are "banner" and "courseleaf".

    Returns:
        int: the load_id associated with this import record. See get_next_load_id for details. Returns -1
            if something goes wrong; 0 if the _imports table is empty; otherwise will return a positive integer.
    """
    try:
        # Create a new import record
        insert_query = QUERIES['reg']['insert'][table_group]['imports']
        load_timestamp = datetime.now()
        params = etl.parameterize_data_frame(pd.DataFrame(data = {'load_timestamp' : [datetime.now()]}))
        etl.insert_to_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, insert_query, parameters = params)
        # Get the new load_id to pass to the remaining functions
        load_id = get_load_id(table_group)
        logger.info(f'A {table_group} import record with load_id = {load_id} was inserted at {load_timestamp}')
    except Exception as e:
        logger.error(f'Unable to get new load_id for {table_group} tables: an exception was raised while connecting to {str(REG_HOSTNAME)}: {str(e)}')
        # should this raise the exception instead of returning a value? TODO figure this out later
        return -1

    return load_id

def get_current_terms():
    """Retrieves codes for the current term, previous term, next term, and next next term from the ORMaintenance database.

    Returns:
        tuple[str]: tuple containing four 6-digit term codes as strings.
    """
    # TODO I think that this can just be a regular tuple and not a namedtuple; I'm only using this
    # method once and do not need keyword access for that usage.
    # Create a namedtuple blueprint
    Terms = namedtuple('Terms', ['current', 'previous', 'next', 'next_next'])
    extract_query = QUERIES['reg']['select']['terms']['current']
    terms = etl.extract_from_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, extract_query)
    # Query results are a tuple wrapped in a list, so need to access the tuple from the list first.
    # Then the values are constructed into a namedtuple to allow keyword access.
    return Terms._make(terms[0]) 
    
# BANNER ETL FUNCTIONS
    
def extract_and_load_banner_courses(load_id):
    """Retrieve course data from Banner, transform results, and load to SQL Server.
    This function queries Banner four times: once each for the current term, previous term, next term, 
    and term after the next term (the next next term). This is the range of terms that will typically
    be of interest to course schedulers.
    """
    terms = get_current_terms()
    course_query = QUERIES['banner']['select']['courses']
    load_query = QUERIES['reg']['insert']['banner']['courses']
    for term in terms:
        try:
            courses = etl.extract_from_oracle(REPTPROD_HOSTNAME, REPTPROD_USERNAME, REPTPROD_PASSWORD, course_query, parameters = [term])
        except Exception as e:
            logger.error(f'BANNER_COURSES: {str(e)}')
        columns = ['term', 'course', 'subject_code', 'course_no', 'course_title', 'college', 'dept_no', 'control_code', 'course_id', 'course_start_term', 'course_end_term', 'course_effective_term', 'status']
        courses = etl.query_results_to_dataframe(courses, columns)
        logger.info(f'BANNER_COURSES: Read {courses.shape[0]} rows from REPTPROD for term {term}.')

        courses.fillna('', inplace = True)
        courses['load_id'] = load_id
        courses['insert_timestamp'] = datetime.now()
        courses = courses[['load_id'] + columns + ['insert_timestamp']]
        logger.info(f'BANNER_COURSES: Prepared {courses.shape[0]} rows for term {term} for load to SQL Server.')

        load_data = etl.parameterize_data_frame(courses)
        try:
            etl.insert_to_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, load_query, load_data)
            logger.info(f'BANNER_COURSES: Load to SQL Server for term {term} complete.')
        except Exception as e:
            logger.error(f'BANNER_COURSES: {str(e)}')

def extract_and_load_banner_departments(load_id):
    """Retrieve academic department data from Banner, transform results, and load to SQL Server.
    """
    extract_query = QUERIES['banner']['select']['departments']
    try:
        departments = etl.extract_from_oracle(REPTPROD_HOSTNAME, REPTPROD_USERNAME, REPTPROD_PASSWORD, extract_query)
    except Exception as e:
        logger.error(f'BANNER_DEPARTMENTS: {str(e)}')
    columns = ['dept_no', 'dept_name']
    departments = etl.query_results_to_dataframe(departments, columns)
    logger.info(f'BANNER_DEPARTMENTS: Read {departments.shape[0]} rows from REPTPROD.')

    departments.fillna('', inplace = True)
    departments['load_id'] = load_id
    departments['insert_timestamp'] = datetime.now()
    departments = departments[['load_id'] + columns + ['insert_timestamp']]
    logger.info(f'BANNER_DEPARTMENTS: Prepared {departments.shape[0]} rows for load to SQL Server.')

    load_query = QUERIES['reg']['insert']['banner']['departments']
    load_data = etl.parameterize_data_frame(departments)
    try:
        etl.insert_to_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, load_query, load_data)
        logger.info(f'BANNER_DEPARTMENTS: Load to SQL Server complete.')
    except Exception as e:
        logger.error(f'BANNER_DEPARTMENTS: {str(e)}')

def extract_and_load_banner_subjects(load_id):
    """Retrieve subject area data from Banner, transform results, and load to SQL Server.
    """
    extract_query = QUERIES['banner']['select']['subjects']
    try:
        subjects = etl.extract_from_oracle(REPTPROD_HOSTNAME, REPTPROD_USERNAME, REPTPROD_PASSWORD, extract_query)
    except Exception as e:
        logger.error(f'BANNER_SUBJECTS: {str(e)}')
    columns = ['subject_code', 'subject', 'subject_name_codebook']
    subjects = etl.query_results_to_dataframe(subjects, columns)
    logger.info(f'BANNER_SUBJECTS: Read {subjects.shape[0]} rows from REPTPROD.')

    subjects.fillna('', inplace = True)
    subjects['load_id'] = load_id
    subjects['insert_timestamp'] = datetime.now()
    subjects = subjects[['load_id'] + columns + ['insert_timestamp']]
    logger.info(f'BANNER_SUBJECTS: Prepared {subjects.shape[0]} rows for load to SQL Server.')

    load_query = QUERIES['reg']['insert']['banner']['subjects']
    load_data = etl.parameterize_data_frame(subjects)
    try:
        etl.insert_to_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, load_query, load_data)
        logger.info(f'BANNER_SUBJECTS: Load to SQL Server complete.')
    except Exception as e:
        logger.error(f'BANNER_SUBJECTS: {str(e)}')

def extract_and_load_banner_terms(load_id):
    """Retrieve academic term data from Banner, transform results, and load to SQL Server.
    """
    extract_query = QUERIES['banner']['select']['terms']
    try:
        terms = etl.extract_from_oracle(REPTPROD_HOSTNAME, REPTPROD_USERNAME, REPTPROD_PASSWORD, extract_query)
    except Exception as e:
        logger.error(f'BANNER_TERMS: {str(e)}')
    columns = ['term_code', 'term_name_full', 'term_name', 'term_start_date', 'term_end_date']
    terms = etl.query_results_to_dataframe(terms, columns)
    logger.info(f'BANNER_TERMS: Read {terms.shape[0]} rows from REPTPROD.')

    terms.fillna('', inplace = True)
    terms['load_id'] = load_id
    terms['insert_timestamp'] = datetime.now()
    terms = terms[['load_id'] + columns + ['insert_timestamp']]
    logger.info(f'BANNER_TERMS: Prepared {terms.shape[0]} rows for load to SQL Server.')

    load_query = QUERIES['reg']['insert']['banner']['terms']
    load_data = etl.parameterize_data_frame(terms)
    try:
        etl.insert_to_sql_server(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, load_query, load_data)
        logger.info(f'BANNER_TERMS: Load to SQL Server complete.')
    except Exception as e:
        logger.error(f'BANNER_TERMS: {str(e)}')

# TODO implement
def extract_and_load_banner_userinfo(load_id):
    logger.warning(f'BANNER_USERINFO: This query has not been implemented.')

# COURSELEAF ETL FUNCTIONS

def extract_and_load_courseleaf_roles(load_id):
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

def extract_and_load_courseleaf_users(load_id):
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

# POWERBI ETL FUNCTIONS

def build_powerbi_tables():
    """ Calls a stored procedyure to create/update the special reference table that Power BI 
    needs to properly display all courses in the same crosslist group.
    """
    sp_query = QUERIES['reg']['stored_procedures']['generate_powerbi_crosslists']
    qf.run_sql_server_query(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, sp_query)

# AGGREGATE FUNCTIONS

def execute_banner_data_load():
    """Same thing as below but for the Banner stuff we care about.
    """
    banner_load_id = insert_new_import_record('banner')

    extract_and_load_banner_courses(banner_load_id)
    extract_and_load_banner_departments(banner_load_id)
    extract_and_load_banner_subjects(banner_load_id)
    extract_and_load_banner_terms(banner_load_id)
    extract_and_load_banner_userinfo(banner_load_id)

    # Manually call SP to update crosslists table after course load is done:
    sp_query = QUERIES['reg']['stored_procedures']['update_current_crosslists']
    qf.run_sql_server_query(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, sp_query)

def execute_courseleaf_data_load():
    """This just runs all of the other CourseLeaf ETL functions. Is this modularity?
    """
    courseleaf_load_id = insert_new_import_record('courseleaf')

    extract_and_load_courseleaf_roles(courseleaf_load_id)
    extract_and_load_courseleaf_users(courseleaf_load_id)

    # Manually call SP to end old roles where called for:
    sp_query = QUERIES['reg']['stored_procedures']['deactivate_old_roles']
    qf.run_sql_server_query(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, sp_query)

def truncate_database_tables():
    """Truncates all application database tables. Only to be used when resetting database for testing purposes.
    Probably dangerous to leave laying around, but whatever.
    """
    # TODO while this might be deleted in a later version of the app, when things are working
    # and there's no reasonable justification for needing to erase all of the data in the 
    # database, I am also tempted to develop a better method for executing all of these 
    # truncates than having an individual query file saved for every single table that needs
    # to be touched. However, that's more of an intellectual exercise and not something I want
    # to pursue right now, hence this overly-wordy comment.

    # Banner
    for table in ['courses', 'departments', 'imports', 'subjects', 'terms', 'userinfo']:
        truncate_query = QUERIES['reg']['truncate']['banner'][table]
        qf.run_sql_server_query(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, truncate_query)
    # Courseleaf
    for table in ['imports', 'roles', 'users']:
        truncate_query = QUERIES['reg']['truncate']['courseleaf'][table]
        qf.run_sql_server_query(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, truncate_query)
    # Current
    for table in ['courses', 'crosslists', 'departments', 'roles', 'subjects', 'users']:
        truncate_query = QUERIES['reg']['truncate']['current'][table]
        qf.run_sql_server_query(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, truncate_query)
    # Power BI
    for table in ['crosslists']:
        truncate_query = QUERIES['reg']['truncate']['powerbi'][table]
        qf.run_sql_server_query(REG_HOSTNAME, REG_USERNAME, REG_PASSWORD, truncate_query)

######################################################################################################
# CODE EXECUTION
######################################################################################################

def main():
    logger.info('COURSELEAF_CONTACTS: Initializing data load.')
    # Always generate new query yaml first to capture any changes made to queries between loads
    QUERIES = qf.load_query_yaml('queries', QUERY_FILE_PATH)

    # This will truncate all app database tables before proceeding with data load.
    # For testing only!!! Remove this before running in production.
    truncate_database_tables()

    # Bring in SQLite file from LeepFrog and confirm that it's accessible
    import_complete = import_database_file()

    # All of the ETL stuff happens here
    if import_complete:
        execute_banner_data_load()
        execute_courseleaf_data_load()
        build_powerbi_tables()
        logger.info('COURSELEAF_CONTACTS: Data load complete.')
    else:
        logger.error('COURSELEAF_CONTACTS: The CourseLeaf database file was not found. Aborting data load.')

def test():
    build_powerbi_tables()

if __name__ == "__main__":
    # test()
    main()
