#!/usr/bin/env python

""" Utility functions for obtaining WDL API key """

import getpass
import sys

from pathlib import Path

def get_api_key_from_keyboard():
    """ Return the API key from keyboard input

    We use the getpass module to prompt the user for their
    WDL API key without echoing their input (to the screen).

    Returns
    -------
    api_key: str
        The API key entered by the user
    """
    api_key = getpass.getpass(prompt='Enter WDL API key: ')

    return api_key

def get_api_key_from_file(filename):
    """ Return the API key from the first line of filename

    Read the api_key from filename.

    Parameters
    ----------
    filename: str | pathlib.Path
        The filename to read to get the api key

    Returns
    -------
    api_key: str
        The API read in from filename
    """
    filename = Path(filename) # Convert filename to a Path object

    api_key = None            # Initialize api_key

    # Use context manager to read the first line of filename
    # Handle common exceptions on file open + read
    try:
        with filename.open(mode='r') as api_file:
            api_key = api_file.readline().strip() # Strip any whitespaces
    except IOError as file_exc:
        # This will handle missing file, bad file type, and permission issues
        err_num, err_str = file_exc.args # pylint: disable=unbalanced-tuple-unpacking
        print(f'I/O error({err_num}): {err_str}')
        raise
    except:
        print('Unexpected error:', sys.exc_info()[0])
        raise

    return api_key
