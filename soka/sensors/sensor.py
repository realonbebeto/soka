from dagster import sensor, RunRequest, SkipReason, AssetKey
from soka.jobs.job import version_job_local, download_job_local, ingest_job_local
import os
from soka.utils.util import convert_dataversion_to_int, get_metadata_version
import kaggle
from soka.core.config import settings

dataset_id = settings.dataset_id

@sensor(name="latest_version_sensor", jobs=[version_job_local, download_job_local], minimum_interval_seconds=30)
def latest_version_sensor(context):
    kaggle.api.authenticate()
    versions = list(kaggle.api.dataset_view(dataset_id).versions)
    version = convert_dataversion_to_int(versions[0])
    
    prev_version = get_metadata_version(context)
    if prev_version is None:
        yield RunRequest(job_name="version_job_local", run_key="fetched_version_"+str(version))
        yield RunRequest(job_name="download_job_local", run_key="downloaded_version_"+str(version))
    elif prev_version:
        if version <= prev_version:
            yield SkipReason("No new version available to download")
            return None
        
        yield RunRequest(job_name="version_job_local", run_key="fetched_version_"+str(version))
        yield RunRequest(job_name="download_job_local", run_key="downloaded_version_"+str(version))


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