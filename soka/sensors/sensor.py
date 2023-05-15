from dagster import sensor, RunRequest, SkipReason
from soka.jobs.job import download_job_local, ingest_job_local
import os
from datetime import datetime

@sensor(name="latest_version_sensor", job=download_job_local, minimum_interval_seconds=30)
def latest_version_sensor():
    old = 0
    new=1
    # # context.assets.get("latest_version").read()
    # fetch_version = latest_version()
    # new = fetch_version()


    if not new > old:
        yield SkipReason("No new version available")
        return

    yield RunRequest(run_key="latest_version_"+str(new))


@sensor(name="check_files_sensor", job=ingest_job_local, minimum_interval_seconds=60)
def check_files_sensor(context):
    """
    Checks if there are any files in the temporary directory to be ingested
    """

    last_mtime = float(context.cursor) if context.cursor else 0
    max_mtime = last_mtime
    directory = './data'

    # Get all files in the directory
    files = os.listdir(directory)

    # Filter only .csv files
    csv_files = [file for file in files if file.endswith('.csv')]

    if len(csv_files) < 8:
        yield SkipReason("No csv files found")
        return None
    
    for csv in csv_files:
        file_path = os.path.join(directory, csv)
        if os.path.isfile(file_path):
            file_mtime = os.stat(file_path).st_mtime
            max_mtime = max(max_mtime, file_mtime)
    
    yield RunRequest(run_key="check_files"+f":{str(max_mtime)}")
    
    context.update_cursor(str(max_mtime))