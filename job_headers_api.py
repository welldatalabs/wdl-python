#!/usr/bin/env python

""" Utility functions for working with the WDL Job Headers API """

from time import sleep

import pandas as pd

import requests

from sqlalchemy import create_engine, exc
from sqlalchemy import Table, Column, Integer, String, Float, DateTime, MetaData

__author__ = "Sam Hoda"
__copyright__ = "Copyright 2019, Well Data Labs"
__version__ = "0.0.1"
__maintainer__ = "Sam Hoda"
__email__ = "sam@welldatalabs.com"
__status__ = "Prototype"

""" The keys of the JOBHeader JSON object returned by the API """
EXPECTED_JOB_HEADERS_COLUMNS = frozenset([
    'api', 'assetGroup', 'bottomholeLatitude', 'bottomholeLongitude',
    'county', 'fleet', 'fluidSystem', 'formation', 'fracSystem', 'jobId',
    'jobStartDate', 'jobType', 'lateralLength', 'lateralLengthUnitText',
    'legalDescription', 'measuredDepth', 'measuredDepthUnitText',
    'modifiedUtc', 'operator', 'padName', 'plannedStages',
    'serviceCompany', 'stageCount', 'state', 'surfaceLatitude',
    'surfaceLongitude', 'verticalDepth', 'verticalDepthUnitText',
    'wellId', 'wellName'])

""" Dictionary to convert JSON keys to Pandas friendly column names """
JOB_HEADERS_COLUMN_MAP = {
    'api': 'api',
    'assetGroup': 'asset_group',
    'bottomholeLatitude': 'bottomhole_latitude',
    'bottomholeLongitude': 'bottomhole_longitude',
    'county': 'county',
    'fleet': 'fleet',
    'fluidSystem': 'fluid_system',
    'formation': 'formation',
    'fracSystem': 'frac_system',
    'jobId': 'job_id',
    'jobStartDate': 'job_start_date',
    'jobType': 'job_type',
    'lateralLength': 'lateral_length',
    'lateralLengthUnitText': 'lateral_length_unit_text',
    'legalDescription': 'legal_description',
    'measuredDepth': 'measured_depth',
    'measuredDepthUnitText': 'measured_depth_unit_text',
    'modifiedUtc': 'modified_utc',
    'operator': 'operator',
    'padName': 'pad_name',
    'plannedStages': 'planned_stages',
    'serviceCompany': 'service_company',
    'stageCount': 'stage_count',
    'state': 'state',
    'surfaceLatitude': 'surface_latitude',
    'surfaceLongitude': 'surface_longitude',
    'verticalDepth': 'vertical_depth',
    'verticalDepthUnitText': 'vertical_depth_unit_text',
    'wellId': 'well_id',
    'wellName': 'well_name'
}

""" The columns to store in the local JobHeaders database. These
    values are expected to be a subset of the values of
    JOB_HEADERS_COLUMN_MAP """
JOB_HEADERS_ALLOWED_COLUMNS = ['job_id', 'modified_utc']

""" The SQLAlchemy datatypes used to create a JobHeaders database """
JOB_HEADERS_DATA_TYPE = {
    'job_id': String,
    'modified_utc': DateTime,
}

def get_api_url():
    """ Return the WDL JobHeaders API endpoint

    The endpoint returned by this module is:

        https://api.welldatalabs.com/jobheaders

    Returns
    -------
    url: str
        The JohHeaders API endpoing
    """
    protocol = 'https'
    base_url = 'api.welldatalabs.com'
    endpoint = 'jobheaders'

    url = f'{protocol}://{base_url}/{endpoint}'
    return url

def get_api_auth_headers(api_key):
    """ Return HTTP Authorization header using WDL API key

    WDL follows the Authorization: <type> <credentials> pattern
    that was introduced by the W3C in HTTP 1.0. That means the
    value of your Authorization header must be set to:

        "Bearer <API Key>".

    The API Key provided by api_key is a secret token that Well
    Data Labs issues to your company or authenticated user. If
    you are an existing Well Data Labs customer, you can obtain
    an API Key for your data from support@welldatalabs.com.

    API Keys allow access to customer data just like a username and
    password. They should be protected and should not be shared.

    Parameters
    ----------
    api_key: str
        The WDL API key to use for request authentication

    Returns
    -------
    headers: dict
        A dictionary containing the HTTP Authorization header
        information to be consumed by the request GET call
    """
    headers = {'Authorization': f'Bearer {api_key}'}
    return headers

def handle_200(response):
    """ Handle 200: return a Pandas dataframe from JSON object

    When the JobHeaders API returns success (200 status_code)
    the response should be a JSON object with the following
    structure:
        {
            "jobId": "b332c113-3397-4379-8ab7-302efc3ae949",
            "wellId": "8d206684-e725-44ca-8235-80ef567804ba",
            "wellName": "Sample Ball-and-Sleeve",
            "api": "05-123-00000-00-00",
            "jobStartDate": "2015-01-01T12:00:00",
            "serviceCompany": "Demo Service Company",
            "fleet": "WDL",
            "operator": "WDL Demo Operator",
            "assetGroup": "WDL Demo Asset Group",
            "formation": "Sample",
            "jobType": "Initial Completion",
            "fracSystem": "Ball and Sleeve",
            "fluidSystem": "Slickwater",
            "bottomholeLatitude": 40.346239,
            "bottomholeLongitude": -104.258837,
            "measuredDepth": 12890,
            "measuredDepthUnitText": "feet",
            "stageCount": 6,
            "padName": "Sample Pad",
            "county": "Weld",
            "state": "CO",
            "surfaceLatitude": 40.338484,
            "surfaceLongitude": -104.256738,
            "legalDescription": "\"legal information can go here.\"",
            "modifiedUtc": "2017-06-20T15:09:06"
        }

    Parameters
    ----------
    response: requests.Response
        The response from the HTTP request

    Returns
    -------
    job_headers_df: pd.DataFrame
        Pandas dataframe containing the results of the request
    """
    assert isinstance(response, requests.Response)
    assert response.status_code == 200

    job_headers_df = pd.DataFrame.from_dict(response.json())
    print(job_headers_df.columns)
    assert isinstance(job_headers_df, pd.DataFrame)
    assert frozenset(job_headers_df.columns) == EXPECTED_JOB_HEADERS_COLUMNS

    return job_headers_df

def handle_400(response):
    """ Handle 400: output warning and suuggested next steps

    Parameters
    ----------
    response: requests.Response
        The response from the HTTP request
    """
    assert isinstance(response, requests.Response)
    assert response.status_code == 400

    print('HTTP 400 Bad Request!')
    print('Please contact support@welldatalabs.com')

def handle_401(response):
    """ Handle 401: output warning and suuggested next steps

    Parameters
    ----------
    response: requests.Response
        The response from the HTTP request
    """
    assert isinstance(response, requests.Response)
    assert response.status_code == 401

    print('HTTP 401 Authentication token is invalid!')
    print('Verify you are correctly setting the HTTP Authorization Header')
    print('and are using the correct WDL API key')

def handle_403(response):
    """ Handle 403: output warning and suuggested next steps

    Parameters
    ----------
    response: requests.Response
        The response from the HTTP request
    """
    assert isinstance(response, requests.Response)
    assert response.status_code == 403

    print('HTTP 403 Forbidden!')
    print('A valid token was received, but it doesn\'t have permissions to JobHeaders')
    print('Contact support@welldatalabs.com for assistance')

def handle_404(response):
    """ Handle 404: output warning and suuggested next steps

    Parameters
    ----------
    response: requests.Response
        The response from the HTTP request
    """
    assert isinstance(response, requests.Response)
    assert response.status_code == 404

    url = response.request.url

    print('HTTP 404 Not Found!')
    print(f'No data found matching the criteria: {url}')

def handle_429(response, default_delay=70):
    """ Handle 429: output warning and suuggested next steps

    The user has exceeded their rate limit. This method will
    pause the program based on the dictated rate-limiting
    logic specified by the API.

    Parameters
    ----------
    response: requests.Response
        The response from the HTTP request

    default_delay: int
        Default number of seconds to wait between requests. This
        number should be non-negative.

    Returns
    -------
    delay: int
        The number of seconds to wait before making another API call.
        This number will be non-negative.
    """
    assert isinstance(response, requests.Response)
    assert response.status_code == 429
    assert default_delay >= 0

    url = response.request.url
    delay = default_delay
    response_retry_delay = response.headers.get('retry-after')

    print('HTTP 429 API throttled')
    if response_retry_delay:
        print(f'Will retry after: {response_retry_delay} seconds...')
        delay = int(response_retry_delay)
    else:
        print(f'Do not know wait time to retry for request: {url}')
        print(f'Continuing with next request after {default_delay} sec throttle window time...')
        delay = default_delay

    assert delay >= 0

    return delay

def handle_generic_response(response):
    """ Handle generic response: output warning and suuggested next steps

    This is reserved for unhandled status codes. Output a snippet of the
    response text to aid the user with debugging.

    Parameters
    ----------
    response: requests.Response
        The response from the HTTP request
    """
    assert isinstance(response, requests.Response)
    url = response.request.url

    print(f'Unhandled HTTP status code: {response.status_code} for request {url}')
    print(response.headers)
    print(response.text[:2000])

def download_job_headers(api_key, default_delay=70, max_attempts=3):
    """ Download JobHeaders data from WDL API as a Pandas DataFrame

    Repeatedly try to download the JobHeaders data from the WDL API.
    On success the JSON object is converted to and returned as a Pandas
    DataFrame. On failure an empty DataFrame is returned.

    Parameters
    ----------
    api_key: str
        The WDL API key to use for request authentication

    default_delay: int
        The default delay between attempts in case a failed attempt
        response does not provide one. default_delay should be
        non-negative

    max_attempts: int
        The maximum number of times to try to make a successful
        API call. This parameter should be positive.

    Returns
    -------
    headers_df: pd.DataFrame
        Pandas DataFrame containing JobHeader JSON object on
        success and empty DataFrame on failure
    """
    assert max_attempts > 0
    assert default_delay >= 0

    url = get_api_url() # Get JobHeaders API url
    headers = get_api_auth_headers(api_key) # Get required request headers for authentication
    headers_df = pd.DataFrame() # headers_df is empty by default
    num_attempts = 0 # Initialize the number of download attempts to zero

    # Retry API call while not exceeding max_attempts
    while num_attempts < max_attempts:

        # Attempt API call and JobHeaders download
        response = requests.get(url, headers=headers) # Make API call
        status_code = response.status_code # Grab the status code
        num_attempts = num_attempts + 1 # Increment the number of attempts
        retry_delay = None # Initialize retry_delay to None 

        # Handle various return codes
        if status_code == 200:
            # On 200 create pd.DataFrame from response
            headers_df = handle_200(response)
        elif status_code == 400:
            handle_403(response)
        elif status_code == 401:
            handle_401(response)
        elif status_code == 403:
            handle_401(response)
        elif status_code == 404:
            handle_404(response)
        elif status_code == 429:
            retry_delay = handle_429(response, default_delay)
        else:
            handle_generic_response(response)
            retry_delay = default_delay

        # Break out of retry loop on success or major failure
        if status_code in frozenset((200, 400, 401, 403, 404)):
            break
        elif retry_delay and num_attempts < max_attempts:
            # Wait for retry_delay seconds before making another attempt
            sleep(retry_delay)

    assert isinstance(headers_df, pd.DataFrame)
    return headers_df

def normalize_column_names(job_headers_df):
    """ Normalize the JobHeader DataFrame column names

    Standardize the column headers using JOB_HEADERS_COLUMN_MAP.

    Parameters
    ----------
    job_headers_df: pd.DataFrame
        Pandas DataFrame containing JobHeader JSON object

    Returns
    -------
    updated_df: pd.DataFrame
        Updated DataFrame with Pandas friendly column names and
    """
    assert isinstance(job_headers_df, pd.DataFrame)
    assert frozenset(job_headers_df.columns) == EXPECTED_JOB_HEADERS_COLUMNS
    assert JOB_HEADERS_COLUMN_MAP.keys() == EXPECTED_JOB_HEADERS_COLUMNS

    updated_df = job_headers_df.copy()

    # Standardize the column headers
    updated_df.columns = [JOB_HEADERS_COLUMN_MAP[column] for column in job_headers_df.columns]
    return updated_df

def normalize_datetimes(job_headers_df):
    """ Normalize the JobHeader DataFrame datetime columns

    Cast job_start_date and modified_utc columns to datetimes.
    The function expects the job_headers_df DataFrame to contain
    job_start_date and modified_utc as columns.

    Parameters
    ----------
    job_headers_df: pd.DataFrame
        Pandas DataFrame containing JobHeader JSON object

    Returns
    -------
    updated_df: pd.DataFrame
        Updated DataFrame with properly cast datetime columns
    """
    assert isinstance(job_headers_df, pd.DataFrame)
    assert 'job_start_date' in job_headers_df.columns
    assert 'modified_utc' in job_headers_df.columns

    updated_df = job_headers_df.copy()

    updated_df = (
        updated_df
        .assign(job_start_date=lambda x: pd.to_datetime(x.job_start_date))
        .assign(modified_utc=lambda x: pd.to_datetime(x.modified_utc)))

    assert 'job_start_date' in updated_df.select_dtypes('datetime').columns
    assert 'modified_utc' in updated_df.select_dtypes('datetime').columns

    return updated_df

def normalize_legal_descriptions(job_headers_df):
    """ Normalize the JobHeader DataFrame legal description column

    Remove the extra quotes in the legal description.
    The function expects the job_headers_df DataFrame to contain
    legal_description as a column.

    Parameters
    ----------
    job_headers_df: pd.DataFrame
        Pandas DataFrame containing JobHeader JSON object

    Returns
    -------
    updated_df: pd.DataFrame
        Updated DataFrame with any extra quotes removed from the
        legal_description column
    """
    assert isinstance(job_headers_df, pd.DataFrame)
    assert 'legal_description' in job_headers_df.columns

    updated_df = job_headers_df.copy()

    updated_df = (
        updated_df
        .assign(legal_description=lambda x: x.legal_description.str.replace('\"', '')))

    return updated_df

def select_allowed_columns(job_headers_df):
    """ Return JobHeader DataFrame with allowed columns

    Return a subset of job_headers_df restricted to the columns
    specified by JOB_HEADERS_ALLOWED_COLUMNS.

    Parameters
    ----------
    job_headers_df: pd.DataFrame
        Pandas DataFrame containing JobHeader JSON object

    Returns
    -------
    updated_df: pd.DataFrame
        Updated DataFrame restricted to columns specified by
        JOB_HEADERS_ALLOWED_COLUMNS
    """
    assert isinstance(job_headers_df, pd.DataFrame)
    assert frozenset(JOB_HEADERS_ALLOWED_COLUMNS) <= frozenset(JOB_HEADERS_COLUMN_MAP.values())

    updated_df = job_headers_df.copy()
    updated_df = updated_df.loc[:, JOB_HEADERS_ALLOWED_COLUMNS]

    assert frozenset(updated_df.columns) == frozenset(JOB_HEADERS_ALLOWED_COLUMNS)

    return updated_df

def get_current_normalized_job_headers(api_key):
    """ Download and return normalized JobHeaders DataFrame

    The key steps are:
        1) Normalize the column names
        2) Normalize the datetime columns
        3) Normalize the legal_description column
        4) Restrict data to allowed columns

    If no data was downloaded the function will return an
    empty DataFrame.

    Parameters
    ----------
    api_key: str
        The WDL API key to use for request authentication

    Returns
    -------
    job_headers_df: pd.DataFrame
        Pandas DataFrame with normalized JobHeader data
    """
    raw_headers_df = download_job_headers(api_key=api_key)

    if raw_headers_df.empty:
        print('Warning: No data downloaded!')
        job_headers_df = raw_headers_df
    else:
        job_headers_df = (raw_headers_df
                          .pipe(normalize_column_names)
                          .pipe(normalize_datetimes)
                          .pipe(normalize_legal_descriptions)
                          .pipe(select_allowed_columns))

    assert isinstance(job_headers_df, pd.DataFrame)

    return job_headers_df

def create_job_headers_table(db_path, table_name):
    """ Create JobHeaders table that stores last update time

    Create a JobHeaders table that has columns for each allowed
    key from the JobHeader JSON object returned by the API. This
    function does NOT load data into the table but only creates
    it if it doesn't already exist.

    The job_id field is a unique identifier for a job and we
    use that as the primary key of the table.

    The table is created in a SQLite database located at dp_path
    and the JobHeaders table's is given by table_name.

    Note: this function will NOT overwrite an existing table in
    db_path with the name table_name.

    Parameters
    ----------
    db_path: str
        The full path of the SQLite table that stores data from
        JobHeaders

    table_name: str
        The name of the table to create
    """
    # Create connection to SQLite db at db_path with minimal logging
    # Instantiate a MetaData object to store Table meta-data
    engine = create_engine(f'sqlite:///{db_path}', echo=False)
    meta = MetaData()

    # Setup meta-data for a table named table_name with job_id as the primary key
    # Only create columns listed in JOB_HEADERS_ALLOWED_COLUMNS
    primary_key = 'job_id'
    primary_key_column = Column(primary_key,
                                JOB_HEADERS_DATA_TYPE[primary_key],
                                primary_key=True)
    non_key_columns = [Column(column, JOB_HEADERS_DATA_TYPE[column])
                       for column in JOB_HEADERS_ALLOWED_COLUMNS if column != primary_key]

    columns = [primary_key_column] + non_key_columns
    _ = Table(table_name, meta, *columns)

    # Create table only if it doesn't already exist
    meta.create_all(engine, checkfirst=True)
    engine.dispose()

def get_existing_job_headers(db_path, table_name):
    """ Return a pandas DataFrame with JobHeaders from SQLite db

    Read data from the table_name table in the SQLite database
    located at db_path. This function will check to make sure that
    the table has the expected columns, i.e., the columns should be
    those in JOB_HEADERS_ALLOWED_COLUMNS.

    Parameters
    ----------
    db_path: str
        The full path of the SQLite table that stores data from
        JobHeaders

    table_name: str
        The name of the JobHeaders table
    """
    # Create connection to SQLite db at db_path with minimal logging
    engine = create_engine(f'sqlite:///{db_path}', echo=False)
    connection = engine.connect()

    # Read table from table_name and store in pd.DataFrame
    existing_job_headers_df = pd.read_sql(table_name, connection)
    engine.dispose()

    # Make sure that table_name has the correct columns
    existing_columns = frozenset(existing_job_headers_df.columns)
    allowed_columns = frozenset(JOB_HEADERS_ALLOWED_COLUMNS)
    assert existing_columns == allowed_columns

    return existing_job_headers_df

def get_missing_job_ids(current_job_headers_df, existing_job_headers_df):
    """ Return the set of job_ids in current_df but not in existing_df

    Identify the set of job_ids present in current_job_headers_df that
    are not present in existing_job_headers_df.

    Parameters
    ----------
    current_job_headers_df: pd.DataFrame
        Pandas DataFrame with the JobHeaders data from the most recent
        JobHeaders API call

    existing_job_headers_df: pd.DataFrame
        Pandas DataFrame with the JobHeaders data storing which jobs
        have been downloaded previously with the relevant meta-data
        (job_id and modified_utc)

    Returns
    -------
    missing_job_ids: frozenset
        Set of job ids present in current_job_headers_df that are not
        present in existing_job_headers_df
    """
    assert isinstance(current_job_headers_df, pd.DataFrame)
    assert isinstance(existing_job_headers_df, pd.DataFrame)
    assert 'job_id' in current_job_headers_df.columns
    assert 'job_id' in existing_job_headers_df.columns

    current_job_ids = frozenset(current_job_headers_df.job_id)
    downloaded_job_ids = frozenset(existing_job_headers_df.job_id)

    missing_job_ids = current_job_ids - downloaded_job_ids

    return missing_job_ids

def get_changed_job_ids(current_job_headers_df, existing_job_headers_df):
    """ Identify jobs where the modified_utc is different between DataFrames

    Compare the modified_utc column for each job present in both
    current_job_headers_df and existing_job_headers_df and return the set
    of job_ids where these times differ.

    Parameters
    ----------
    current_job_headers_df: pd.DataFrame
        Pandas DataFrame with the JobHeaders data from the most recent
        JobHeaders API call

    existing_job_headers_df: pd.DataFrame
        Pandas DataFrame with the JobHeaders data storing which jobs
        have been downloaded previously with the relevant meta-data
        (job_id and modified_utc)

    Returns
    -------
    changed_job_ids: frozenset
        Set of job ids with a different modified_utc time when comparing
        current_job_headers_df to existing_job_headers_df
    """
    columns = ['job_id', 'modified_utc']
    required_columns = frozenset(columns)

    assert isinstance(current_job_headers_df, pd.DataFrame)
    assert isinstance(existing_job_headers_df, pd.DataFrame)
    assert required_columns <= frozenset(current_job_headers_df.columns)
    assert required_columns <= frozenset(existing_job_headers_df.columns)

    # Inner join on both DataFrames to isolate common job_ids
    # Add suffix _current to to identify columns from current_job_headers_df
    # and the suffix _existing to identify columns from existing_job_headers_df
    common_df = current_job_headers_df[columns].merge(existing_job_headers_df[columns],
                                                      how='inner',
                                                      on='job_id',
                                                      suffixes=('_current', '_existing'))

    # Identify rows with common job_ids but different modified dates
    changed_df = common_df.query('modified_utc_current != modified_utc_existing')

    # Return the target set of job_ids
    changed_job_ids = frozenset(changed_df.job_id)
    return changed_job_ids

def identify_job_ids_to_download(current_job_headers_df, existing_job_headers_df):
    """ Identify job_ids to download

    The job_ids that need to be downloaded are:

        1) Jobs that are in the current_job_headers_df but not in
           existing_job_headers_df, and
        2) Jobs common to both the current_job_headers_df and
           existing_job_headers_df that have different modified dates
           (indicated by different modified_utc values)

    Note: You could grab the new job_ids using a left join in
    get_changed_job_ids() but we prefer to be explicit using
    get_missing_job_ids()

    Parameters
    ----------
    current_job_headers_df: pd.DataFrame
        Pandas DataFrame with the JobHeaders data from the most recent
        JobHeaders API call

    existing_job_headers_df: pd.DataFrame
        Pandas DataFrame with the JobHeaders data storing which jobs
        have been downloaded previously with the relevant meta-data
        (job_id and modified_utc)

    Returns
    -------
    job_ids_to_download: frozenset
        Set of job_ids to download from the API
    """
    required_columns = frozenset(('job_id', 'modified_utc'))

    assert isinstance(current_job_headers_df, pd.DataFrame)
    assert isinstance(existing_job_headers_df, pd.DataFrame)
    assert required_columns <= frozenset(current_job_headers_df.columns)
    assert required_columns <= frozenset(existing_job_headers_df.columns)

    missing_job_ids = get_missing_job_ids(current_job_headers_df, existing_job_headers_df)
    changed_job_ids = get_changed_job_ids(current_job_headers_df, existing_job_headers_df)

    # The set of jobs to download is the union of the missing jobs and changed jobs
    job_ids_to_download = missing_job_ids | changed_job_ids

    return job_ids_to_download

def get_jobs_to_download(api_key, db_path, table_name):
    """ Return a pd.DataFrame of jobs that need to be downloaded

    The job that need to be downloaded are those that

    1) have not been downloaded previously (according to the records in
       the table given by table_name in the database located at db_path),
    2) have been downloaded previously but have been modified since the
       last download (jobs that are out of sync)

    The database is a SQLite database and db_path should be a full pathname,
    e.g., '/directory_1/sub_directory/wdl_job_headers.db'. If you simply use
    'wdl_job_headers.db' then the database will be located in the directory of
    the calling script or notebook.

    We recommend setting the table_name to 'job_headers'.

    The function will create the database and table if they do not already
    exist.

    Parameters
    ----------
    api_key: str
        The WDL API key to use for request authentication

    db_path: str
        The full path of the SQLite table that stores data from
        JobHeaders

    table_name: str
        The name of the JobHeaders table

    Returns
    -------
    jobs_to_download_df: pd.DataFrame
        A pd.DataFrame of jobs that need to be downloaded to be in
        sync with current WDL records
    """
    # Create job headers database and table if they don't exist
    create_job_headers_table(db_path=db_path, table_name=table_name)

    # Make API call to get updated WDL JobHeaders information
    current_job_headers_df = get_current_normalized_job_headers(api_key=api_key)

    # Add required columns to current_job_headers_df when it is empty
    if current_job_headers_df.empty:
        required_columns = ['job_id', 'modified_utc']
        current_job_headers_df = pd.DataFrame(data=None, columns=required_columns)

    # Get the JobHeaders information stored in the local database
    existing_job_headers_df = get_existing_job_headers(db_path=db_path,
                                                       table_name=table_name)

    # Compare the existing information to the new information to determine
    # the set of jobs that are missing or out of sync and need to be
    # downloaded
    job_ids_to_download = identify_job_ids_to_download( #pylint: disable=unused-variable
        current_job_headers_df=current_job_headers_df,
        existing_job_headers_df=existing_job_headers_df)

    # Filter current_job_headers_df to only those job ids that
    # need to be downloaded
    jobs_to_download_df = (current_job_headers_df
                           .query('job_id in @job_ids_to_download')
                           .reset_index(drop=True))

    return jobs_to_download_df

def update_job_headers_db_row(job_headers_df, job_id, db_path, table_name):
    """ Update local db for job_id using info from job_headers_df

    Update the local database JobHeaders table (identified by db_path and
    table_name) for job_id using information from job_headers_df.

    One intended use of this function is to update the local JobHeaders table
    when the PerSec data for job_id has been successfully downloaded. By using
    the data in jobs_to_download_df (JobHeaders data from the most recent API
    call, i.e., a subset of the DataFrame returned from
    get_current_normalized_job_headers()) we will update the modified_utc column
    in the local table to reflect our download.

    Parameters
    ----------
    job_headers_df: pd.DataFrame
        Pandas DataFrame with the JobHeaders data used to update
        the database table specified by db_path and table_name

    job_id: str
        The job_id to to update

    db_path: str
        The full path of the SQLite table that stores data from
        JobHeaders

    table_name: str
        The name of the JobHeaders table
    """
    assert isinstance(job_headers_df, pd.DataFrame)
    assert 'job_id' in job_headers_df.columns

    # Find job_id row in JobHeaders pd.DataFrame
    current_row_df = job_headers_df.query('job_id == @job_id')

    # If there is no row corresponding to job_id output a warning and return
    if current_row_df.empty:
        print(f'(update_job_headers_db_row) job_id {job_id} missing!')
        return

    # Create connection to SQLite db at db_path with minimal logging
    engine = create_engine(f'sqlite:///{db_path}', echo=False)
    connection = engine.connect()

    # Start a transaction for the deletion
    #
    # We should not include the pd.DataFrame.to_sql() in the transaction
    # because we may end up with nested transactions
    #
    # If the insert(append) fails then at worst we will identify the missing
    # job_id during the next JobHeaders update.
    trans = connection.begin()

    try:
        # delete row that we are going to "upsert"
        engine.execute(f'delete from {table_name} where job_id="{job_id}"')
        trans.commit()

        # insert changed row
        current_row_df.to_sql(table_name, engine, if_exists='append', index=False)
    except exc.SQLAlchemyError:
        # On an exception rollback the transaction
        trans.rollback()
    finally:
        # Clean up after ourselves
        engine.dispose()

    return
