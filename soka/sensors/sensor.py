import os

from dagster import RunRequest, SkipReason, sensor

from soka.core.config import settings
from soka.jobs.job import download_job_local, ingest_job_local
from soka.utils.util import find_latest_version
from datetime import datetime, timezone

owner_slug, dataset_slug = settings.dataset_id.split("/")
START_VERSION = os.getenv("START_VERSION", 500)


@sensor(name="check_raw_files", jobs=[download_job_local], minimum_interval_seconds=60)
def check_raw_files(context):
    last_version = int(context.cursor if context.cursor else START_VERSION)
    curr_version = find_latest_version(owner_slug, dataset_slug, last_version)

    if curr_version <= last_version:
        yield SkipReason("No new version available to download")
        return

    context.log.info(f"Current version: {curr_version}")
    yield RunRequest(
        job_name="download_job_local", run_key=f"downloaded_version_{curr_version}"
    )

    # Update the cursor to mark progress
    context.update_cursor(str(curr_version))


@sensor(
    name="check_ingest_files", jobs=[ingest_job_local], minimum_interval_seconds=120
)
def check_ingest_files(context):
    """
    Checks if there are any files in the temporary directory to be ingested
    """

    directory = "./data"

    # Get all files in the directory
    files = os.listdir(directory)

    # Filter only .csv files
    csv_files = [file for file in files if file.endswith(".csv")]

    run_time = int(datetime.now(timezone.utc).timestamp())

    if len(csv_files) < 8:
        yield SkipReason("No csv files found")
        context.update_cursor(str(run_time))
        return None

    yield RunRequest(
        job_name="ingest_job_local", run_key="check_files" + f":{str(run_time)}"
    )

    context.update_cursor(str(run_time))
