## Overview
Well Data Labs provides a RESTful API that allows customers access to their data.  The Python SDK is intended as a starting point for customers to download data via the Well Data Labs API.

The SDK includes an example Process.py file with a basic working sample using the test API authorization key.  The samples demonstrate how to make calls to the PerSecData API and how to consume the results.

## Setup
This SDK utilizes SQLAlchemy and SQLite to hold the state of the Jobs that have been downloaded.  You will need to install SQLite and pass in the Database file as a parameter as seen in the process.py script.  To install SQLite you can look at the following Sites:

- https://sqlitebrowser.org/
- https://www.sqlite.org/

SQLAlchemy can be installed using: pip install sqlalchemy
Other dependencies installed using PIP are: requests and pandas

## Run
```
> python process.py
```

This process workflow is defined as follows:
1) Run: job_headers_api.get_jobs_to_download
   - Request JobHeaders data from the WDL API: https://api.welldatalabs/jobheaders.
   - Save the Job_Id and Modified_Utc from this result into SQLite table.
   - Returns jobs that need to be downloaded based off a change in the Modified_Utc previouslly saved in SQLite.
2) Run: persec_data_api.download_persec_data
   - Request the PerSec data for the Jobs that need to be downloaded from the WDL API: https://api.welldatalabs/persecdata.
   - Saves all PerSec results as CSV output to the specified path given.

## API Documentation
Well Data Labs API documentation can be found here.
https://api.welldatalabs.com/docs

## Support
If you have any issues or questions working with the Well Data Labs API, please contact support support@welldatalabs.com.
