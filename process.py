import job_headers_api
import persec_data_api

def process():
    # Creates a table called JobHeaders in SQLite and pulls data from https://api.welldatalabs.com/jobheaders into DataFrame
    jobs_to_download_df = job_headers_api.get_jobs_to_download('b+S15uKWEK0lFU+NomEmvekn8yk/ALTTBAYOJalVKrI=', 'sqlite/wdl-api.db', 'JobHeaders')
    
    # Loop through DataFrame and update values into SQLite database
    for job_id in jobs_to_download_df.job_id.values:
        job_headers_api.update_job_headers_db_row(jobs_to_download_df, job_id, 'sqlite/wdl-api.db', 'JobHeaders')

    # Download PerSecData from https://api.welldatalabs.com/persecdata
    persec_data_api.download_persec_data(jobs_to_download_df, 'b+S15uKWEK0lFU+NomEmvekn8yk/ALTTBAYOJalVKrI=', './persecfiles')

if __name__ == "__main__":
    process()
