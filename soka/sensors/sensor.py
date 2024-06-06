from dagster import sensor, RunRequest, SkipReason
from soka.jobs.job import version_job_local, download_job_local, ingest_job_local
import os
from soka.utils.util import get_metadata_version, try_for_version
from soka.core.config import settings

import copy

owner_slug, dataset_slug = settings.dataset_id.split("/")
start_version = 428


def ingest_request(context):
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
        context.update_cursor(str(max_mtime))
        return None
    
    for csv in csv_files:
        file_path = os.path.join(directory, csv)
        if os.path.isfile(file_path):
            file_mtime = os.stat(file_path).st_mtime
            max_mtime = max(max_mtime, file_mtime)
    
    yield RunRequest(run_key="check_files"+f":{str(max_mtime)}")
    
    context.update_cursor(str(max_mtime))

@sensor(name="download_ingest_sensor", jobs=[version_job_local, download_job_local, ingest_job_local], minimum_interval_seconds=60)
def download_ingest_sensor(context):
    global start_version
    curr_version = get_metadata_version(context, "latest_version")
    tmp_curr = copy.deepcopy(curr_version)

    if curr_version is None:
        curr_version = start_version
        run_config = {"ops": {"record_version": {"config": {"version": curr_version}}}}
        yield RunRequest(job_name="download_job_local", run_key=f"downloaded_version_{curr_version}")
        yield RunRequest(job_name="version_job_local", run_key=f"recorded_version_{curr_version}", run_config=run_config)
        ingest_request(context)

    else:
        curr_version = try_for_version(owner_slug, dataset_slug, curr_version+1)

        if not curr_version or curr_version <= tmp_curr:
            yield SkipReason("No new version available to download")
            return
        
        context.log.info(f"Current version: {curr_version}")
        run_config = {"ops": {"record_version": {"config": {"version": curr_version}}}}
        yield RunRequest(job_name="download_job_local", run_key=f"downloaded_version_{curr_version}")
        yield RunRequest(job_name="version_job_local", run_key=f"recorded_version_{curr_version}", run_config=run_config)
        ingest_request(context)
