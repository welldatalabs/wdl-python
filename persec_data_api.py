#!/usr/bin/env python

""" Utility functions for working with the WDL PerSecData API """

from io import StringIO
from time import sleep
from collections import namedtuple
from functools import partial
from pathlib import Path
from types import FunctionType

import pandas as pd

import requests

PerSecFilenames = namedtuple('PerSecFilenames',
                             'raw_filename formatted_filename units_filename')
PerSecFilenames.__doc__ = """\
    A tuple storing target filenames for various possible CSVs for a job

    The module uses this named tuple to pass target filenames for storing the:

        1) raw PerSec CSV data,
        2) formatted PerSec CSV data, and
        3) PerSec units data An empty

    An empty string or None entry indicates the user does not want to save
    the data in the corresponding file type (raw/formatted/units). The entries
    can be pathlib.Path objects as well as strings.

    Parameters
    ----------
    raw: str | None | pathlib.Path
        The filename to store the raw CSV data

    formatted: str | None | pathlib.Path
        The filename to store the formatted CSV data

    units: str | None | pathlib.Path
        The filename to store the units data
"""

def get_api_url(job_id):
    """ Return the WDL PersecData API endpoint

    The endpoint returned by this module is:

        https://api.welldatalabs.com/persecdata/<job_id>

    Parameters
    ----------
    job_id: str
        The job_id to search the PerSec API for

    Returns
    -------
    url: str
        The PersecData API endpoing
    """
    protocol = 'https'
    base_url = 'api.welldatalabs.com'
    endpoint = 'persecdata'

    url = f'{protocol}://{base_url}/{endpoint}/{job_id}'
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
    assert isinstance(api_key, str)

    headers = {'Authorization': f'Bearer {api_key}'}
    return headers

def save_raw_persec_data(csv_data, filename):
    """ Save the raw CSV to filename

    The raw CSV has the following structure:

        JOB TIME,JOB TIME0,STAGE TIME0,TIME TO ISIP,WELL NAME,\
        API NUMBER,STAGE NUMBER,TREATING PRESSURE,BOTTOMHOLE PRESSURE,\
        ANNULUS PRESSURE,SURFACE PRESSURE,SLURRY RATE,CLEAN VOLUME,\
        SLURRY VOLUME,PROPPANT TOTAL,PROPPANT CONC,BOTTOMHOLE PROPPANT CONC
        (datetime),(min),(min),(min),(none),(none),(none),(psi),(psi),(psi),\
        (psi),(bpm),(bbl),(bbl),(lbs),(lbs/gal),(lbs/gal)
        06/17/18 04:15:08,0.033333,0.000000,,Sample Ball-and-Sleeve,\
        05-123-00000-00-00,1,-16.310000,0.000000,-8.130000,-0.900000,\
        0.480000,0.000000,7.300000,0.000000,0.000000,0.000000
        06/17/18 04:15:09,0.050000,0.016667,,Sample Ball-and-Sleeve,\
        05-123-00000-00-00,1,-16.310000,0.000000,-8.130000,2.710000,\
        0.490000,0.000000,7.310000,0.000000,0.000000,0.000000
        ...

    This function saves the raw CSV to filename.

    Note: not every PerSecData CSV files has the same columns.

    Parameters
    ----------
    csv_data: str
        The CSV data as a string

    filename: str or pathlib.Path
        The target filename for storing the raw CSV data
    """
    assert isinstance(csv_data, str)
    assert isinstance(filename, str) or isinstance(filename, Path)

    with open(filename, 'w') as csv_file:
        csv_file.write(csv_data)

def format_persec_column_label(label):
    """ Return a re-formatted PerSecData column label

    This function converts column labels to snake case (all lower-case
    and spaces converted to underscores).

    Parameters
    ----------
    label: str
        The original column label to be formatted

   Returns
   -------
   formatted_label: str
        The formatted label
    """
    assert isinstance(label, str)

    formatted_label = label.lower().replace(' ', '_')

    assert formatted_label.islower() and ' ' not in formatted_label

    return formatted_label

def save_formatted_persec_data(csv_data, filename):
    """ Save a mildly formatted version of csv_data to filename

    The raw CSV has the following structure:

        JOB TIME,JOB TIME0,STAGE TIME0,TIME TO ISIP,WELL NAME,\
        API NUMBER,STAGE NUMBER,TREATING PRESSURE,BOTTOMHOLE PRESSURE,\
        ANNULUS PRESSURE,SURFACE PRESSURE,SLURRY RATE,CLEAN VOLUME,\
        SLURRY VOLUME,PROPPANT TOTAL,PROPPANT CONC,BOTTOMHOLE PROPPANT CONC
        (datetime),(min),(min),(min),(none),(none),(none),(psi),(psi),(psi),\
        (psi),(bpm),(bbl),(bbl),(lbs),(lbs/gal),(lbs/gal)
        06/17/18 04:15:08,0.033333,0.000000,,Sample Ball-and-Sleeve,\
        05-123-00000-00-00,1,-16.310000,0.000000,-8.130000,-0.900000,\
        0.480000,0.000000,7.300000,0.000000,0.000000,0.000000
        06/17/18 04:15:09,0.050000,0.016667,,Sample Ball-and-Sleeve,\
        05-123-00000-00-00,1,-16.310000,0.000000,-8.130000,2.710000,\
        0.490000,0.000000,7.310000,0.000000,0.000000,0.000000
        ...

    This function converts the header to snake case (all lower-case and
    spaces converted to underscores) using format_persec_column_label().
    It also casts the JobTime column to a Pandas datatime column prior to
    saving the CSV forcing the datatime format to adhere to "%m/%d/%y %H:%M:%S".

    The function also skips the units row. The reason is that the units
    row prevents Python/R from correctly inferring the data type of the
    columns when they are read in.

    Note: not every PerSecData CSV files has the same columns.

    Parameters
    ----------
    csv_data: str
        The CSV data as a string

    filename: str or pathlib.Path
        The target filename for storing the formatted CSV data
    """
    assert isinstance(csv_data, str)
    assert isinstance(filename, str) or isinstance(filename, Path)

    # Read csv_data into a pd.DataFrame skipping the units row
    persec_df = pd.read_csv(StringIO(csv_data), skiprows=[1])

    # Re-format the column headers
    persec_df.columns = [format_persec_column_label(column)
                         for column in persec_df.columns]

    # Format the job_time column
    assert 'job_time' in persec_df.columns
    persec_df = (persec_df
                 .assign(job_time=lambda x: pd.to_datetime(x.job_time,
                                                           format='%m/%d/%y %H:%M:%S')))

    # Write persec_df to filename without default Pandas index column
    persec_df.to_csv(filename, index=False)

def save_persec_units_data(csv_data, filename):
    """ Save the units data in csv_data to filename

    The raw CSV has the following structure:

        JOB TIME,JOB TIME0,STAGE TIME0,TIME TO ISIP,WELL NAME,\
        API NUMBER,STAGE NUMBER,TREATING PRESSURE,BOTTOMHOLE PRESSURE,\
        ANNULUS PRESSURE,SURFACE PRESSURE,SLURRY RATE,CLEAN VOLUME,\
        SLURRY VOLUME,PROPPANT TOTAL,PROPPANT CONC,BOTTOMHOLE PROPPANT CONC
        (datetime),(min),(min),(min),(none),(none),(none),(psi),(psi),(psi),\
        (psi),(bpm),(bbl),(bbl),(lbs),(lbs/gal),(lbs/gal)
        06/17/18 04:15:08,0.033333,0.000000,,Sample Ball-and-Sleeve,\
        05-123-00000-00-00,1,-16.310000,0.000000,-8.130000,-0.900000,\
        0.480000,0.000000,7.300000,0.000000,0.000000,0.000000
        06/17/18 04:15:09,0.050000,0.016667,,Sample Ball-and-Sleeve,\
        05-123-00000-00-00,1,-16.310000,0.000000,-8.130000,2.710000,\
        0.490000,0.000000,7.310000,0.000000,0.000000,0.000000
        ...

    The function reads in the first two rows of csv_data, i.e., the
    header row and the units row.  It then converts the header to snake
    case (all lower-case and spaces converted to underscores) using
    format_persec_column_label().  It also removes the parentheses
    surrounding the units in the second row of the CSV data.
    Finally, the updated header and units row are written as a CSV
    filename specified by filename.

    The units CSV file looks like:

        job_time,job_time0,stage_time0,time_to_isip,well_name,\
        api_number,stage_number,treating_pressure,bottomhole_pressure,\
        annulus_pressure,surface_pressure,slurry_rate,clean_volume,\
        slurry_volume,proppant_total,proppant_conc,bottomhole_proppant_conc
        dattime,min,min,min,none,none,none,psi,psi,psi,\
        psi,bpm,bbl,bbl,lbs,lbs/gal,lbs/gal

    Note: not every PerSecData CSV files has the same columns.

    Parameters
    ----------
    csv_data: str
        The CSV data as a string

    filename: str or pathlib.Path
        The target filename for storing the units data
    """
    def strip_parentheses(unit):
        """ Remove parentheses from unit str """
        return unit.translate({ord(i): None for i in '()'})

    assert isinstance(csv_data, str)
    assert isinstance(filename, str) or isinstance(filename, Path)

    # Read the first two rows of csv_data into a pd.DataFrame
    units_df = pd.read_csv(StringIO(csv_data), nrows=1)

    # Re-format the column headers
    updated_columns = [format_persec_column_label(column)
                       for column in units_df.columns]

    # Strip parentheses from the units
    updated_units = [strip_parentheses(unit) for unit in units_df.iloc[0]]

    # Build a new DataFrame from re-formatted header and units row
    units_df = pd.DataFrame(data=[updated_units], columns=updated_columns)

    # Write units_df to filename without default Pandas index column
    units_df.to_csv(filename, index=False)

def handle_200(response, persec_filenames):
    """ Handle 200: return a Pandas dataframe from JSON object

    When the JobHeaders API returns success (200 status_code)
    the response text should be CSV file.

    Three different CSVs can be written:

        1) The raw CSV: complete original CSV
        2) The formatted CSV: snake case header and no units row
        3) The units CSV: snake case header and units row

    An empty string or None key in persec_filenames will cause
    that file type to be skipped.

    Parameters
    ----------
    response: requests.Response
        The response from the HTTP request

    persec_filenames: PerSecFilenames
        The target filenames for the raw CSV, formatted CSV, and
        units CSV. An empty or None entry skips writting that
        file type.
    """
    # Basic pre-conditions for function
    assert isinstance(response, requests.Response)
    assert isinstance(persec_filenames, PerSecFilenames)
    assert response.status_code == 200

    # Extract CSV data stored in response
    csv_data = response.text

    # Save raw CSV if a raw filename is provided
    if persec_filenames.raw_filename:
        save_raw_persec_data(csv_data=csv_data,
                             filename=persec_filenames.raw_filename)

    # Save formatted CSV if a formatted filename is provided
    if persec_filenames.formatted_filename:
        save_formatted_persec_data(csv_data=csv_data,
                                   filename=persec_filenames.formatted_filename)

    # Save units CSV if a units filename is provided
    if persec_filenames.units_filename:
        save_persec_units_data(csv_data=csv_data,
                               filename=persec_filenames.units_filename)

def handle_400(response):
    """ Handle 400: output warning and suuggested next steps

    There was a change during the week of 8/29/2019 where status code
    417 would be returned for jobs that had issues being parsed, e.g.,
    the job loader could not detect a job start time. The API now just
    returns 400.

    Parameters
    ----------
    response: requests.Response
        The response from the HTTP request
    """
    assert isinstance(response, requests.Response)
    assert response.status_code == 400

    print('HTTP 400 Bad Request!')

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
    print('A valid token was received, but it doesn\'t have permissions to PerSecData')
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

    The user has exceeded their rate limit. This method will return the
    number of seconds the caller should wait based on the dictated
    rate-limiting logic specified by the API.

    Parameters
    ----------
    response: requests.Response
        The response from the HTTP request

    default_delay: int
        Default number of seconds to wait between requests. This
        integer should be non-negative.

    Returns
    -------
    delay: int
        The number of seconds the caller should wait until the
        next PerSecData API call. This number will be non-negative.
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

    default_delay: int
        Default number of seconds to wait between requests
    """
    assert isinstance(response, requests.Response)
    url = response.request.url

    print(f'Unhandled HTTP status code: {response.status_code} for request {url}')
    print(response.headers)
    print(response.text[:2000])

def download_job_persec(job_id, api_key, persec_filenames,
                        default_delay=70, max_attempts=3):
    """ Download PerSecData for job_id and save CSVs given by persec_filenames

    Repeatedly try to download the PerSecData data for the job indexed
    by job_id from the WDL API.  On success, the PerSecData is save in
    files according to the
    entires of persec_filenames.

    Parameters
    ----------
    job_id: str
        The JobId used to search the PerSecAPI

    api_key: str
        The WDL API key to use for request authentication

    persec_filenames: PerSecFilenames
        The target filenames for the raw CSV, formatted CSV, and
        units CSV. An empty or None entry skips writting that
        file type.

    default_delay: int
        Default number of seconds to wait between requests. This
        number should be non-negative.

    max_attempts: int
        The maximum number of times to attempt the download the
        PerSecData for job_id. This number should be positive.

    Returns
    -------
    download_successful: bool
        Was the API call and CSV save successful
    """
    assert isinstance(max_attempts, int) and max_attempts > 0
    assert isinstance(default_delay, int) and default_delay >= 0

    url = get_api_url(job_id)                  # Get the PerSecData url for job_id
    headers = get_api_auth_headers(api_key)    # Get the request headers for authentication

    status_code = None                         # Initialize the response status code
    delay_before_next_api_call = default_delay # Initialize delay to default
    download_successful = False                # Initialize the success flag to False

    num_attempts = 0                           # Initialize the number of attempts to zero

    # Retry while not exceeding max_attempts
    while num_attempts < max_attempts:

        print(job_id)
        # Attempt API call and PerSecData download and save
        response = requests.get(url, headers=headers) # Make API call
        status_code = response.status_code            # Grab the status code
        num_attempts = num_attempts + 1               # Increment the number of attempts

        # Handle various return codes
        if status_code == 200:
            # On 200 create CSV files from response according to persec_filenames
            handle_200(response, persec_filenames)
            download_successful = True
        elif status_code == 400:
            handle_400(response)
        elif status_code == 401:
            handle_401(response)
        elif status_code == 403:
            handle_403(response)
        elif status_code == 404:
            handle_404(response)
        elif status_code == 429:
            delay_before_next_api_call = handle_429(response, default_delay)
        else:
            handle_generic_response(response)
            delay_before_next_api_call = default_delay

        # Break out of retry loop on success or major failure otherwise
        # wait before making the next API call
        if status_code in frozenset((200, 400, 401, 403, 404)):
            break
        elif num_attempts < max_attempts:
            sleep(delay_before_next_api_call)

    # Was the API call and download successful
    return download_successful

def default_raw_csv_filename(job_id):
    """ Returns the filename for the raw CSV associated with job_id

    Parameters
    ----------
    job_id: str
        The job_id to generate the filename for

    Returns
    -------
    filename: pathlib.Path
        The filename associated with job_id which will be appended
        to some base path by the caller.
    """
    assert isinstance(job_id, str)

    return Path(f'original_{job_id}.csv')

def default_formatted_csv_filename(job_id):
    """ Returns the filename for the formatted CSV associated with job_id

    Parameters
    ----------
    job_id: str
        The job_id to generate the filename for

    Returns
    -------
    filename: pathlib.Path
        The filename associated with job_id which will be appended
        to some base path by the caller.
    """
    assert isinstance(job_id, str)

    return Path(f'formatted_{job_id}.csv')

def default_units_csv_filename(job_id):
    """ Returns the filename for the units CSV associated with job_id

    Parameters
    ----------
    job_id: str
        The job_id to generate the filename for

    Returns
    -------
    filename: pathlib.Path
        The filename associated with job_id which will be appended
        to some base path by the caller.
    """
    assert isinstance(job_id, str)

    return Path(f'units_{job_id}.csv')

def nosave_filename(job_id): #pylint:disable=unused-argument
    """ Returns None to indicate not to save a CSV file

    Parameters
    ----------
    job_id: str
        The job_id associated with the job being processed
    """
    return None

def download_persec_data(job_header_df, api_key, base_path,
                         raw_filename_function=default_raw_csv_filename,
                         formatted_filename_function=default_formatted_csv_filename,
                         units_filename_function=default_units_csv_filename,
                         local_job_headers_updater=None,
                         default_delay=70, max_attempts=3):
    """ Download jobs in job_header_df from PerSecData API

    Parameters
    ----------
    job_header_df: pd.DataFrame
        A DataFrame with the a column of job_ids to download from the
        PerSecData API

    api_key: str
        The WDL API key to use for request authentication

    base_path: pathlib.Path or str
        The base path to use for the target CSV files

    raw_filename_function: Callable[[str], pathlib.Path]
        A function that takes a job_id and produces a filename for the
        raw CSV data that is appended to base_path. This can be
        set to persec_data_api.nosave_filename to not save the
        file.

    formatted_filename_function: Callable[[str], pathlib.Path]
        A function that takes a job_id and produces a filename for the
        formatted CSV data that is appended to base_path. This can be
        set to persec_data_api.nosave_filename to not save the
        file.

    units_filename_function: Callable[[str], pathlib.Path]
        A function that takes a job_id and produces a filename for the
        units CSV data that is appended to base_path. This can be
        set to persec_data_api.nosave_filename to not save the
        file.

    local_job_headers_updater: Callable[[str], None]
        A function that takes a job_id and updates a local JobHeaders
        database table.

    default_delay: int
        Default number of seconds to wait between requests. This
        number should be non-negative.

    max_attempts: int
        The maximum number of times to attempt the download the
        PerSecData for job_id. This number should be positive.
    """
    def is_function(param):
        return isinstance(param, (FunctionType, partial))

    def prepend_base_path(filename):
        return base_path / filename if filename else None

    # Check basic pre-conditions
    assert isinstance(job_header_df, pd.DataFrame)
    assert 'job_id' in job_header_df.columns
    assert isinstance(api_key, str)
    assert is_function(raw_filename_function)
    assert is_function(formatted_filename_function)
    assert is_function(units_filename_function)
    assert is_function(local_job_headers_updater) or local_job_headers_updater is None
    assert default_delay >= 0
    assert max_attempts > 0
    
    # Cast base_path to pathlib.Path object
    base_path = Path(base_path)

    # There is no delay when making the first call
    delay_before_making_next_api_call = 0
    
    # Loop over all the jobs marked for download
    for job_id in job_header_df.job_id.values:
        
        # Store the target CSV filenames
        raw_filename = prepend_base_path(raw_filename_function(job_id))
        formatted_filename = prepend_base_path(formatted_filename_function(job_id))
        units_filename = prepend_base_path(units_filename_function(job_id))

        persec_filenames = PerSecFilenames(raw_filename=raw_filename,
                                           formatted_filename=formatted_filename,
                                           units_filename=units_filename)
        
        # Wait the appropriate amount of time before making the API call
        sleep(delay_before_making_next_api_call)

        # Make the API call
        download_success = download_job_persec(job_id=job_id,
                                               api_key=api_key,
                                               persec_filenames=persec_filenames,
                                               default_delay=default_delay,
                                               max_attempts=max_attempts)

        # Update the local JobHeaders DB entry when the API call and download was successful
        if download_success and local_job_headers_updater is not None:
            local_job_headers_updater(job_id=job_id)

        # Set the delay for making the next API call
        delay_before_making_next_api_call = default_delay
