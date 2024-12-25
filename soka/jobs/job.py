from dagster import graph
from soka.ops.op import (
    download_dataset,
    ingest_datasets_raw,
    create_dim_dates,
)
from soka.resources.resource import postgres_resource
from soka.core.config import settings

dev_local = {
    "ops": {"ingest_datasets_raw": {"config": {"dir": "./data"}}},
}

db_config = {
    "resources": {
        "database": {
            "config": {
                "host": settings.db_host,
                "port": settings.db_port,
                "user": settings.db_username,
                "password": settings.db_password,
                "database": settings.db_name,
            }
        }
    }
}



@graph
def download_job():
    # download dataset
    download_dataset()


@graph
def ingest_job():
    ingest_datasets_raw()


@graph
def dates_job():
    create_dim_dates()


download_job_local = download_job.to_job(name="download_job_local", tags={"dev": True})


ingest_job_local = ingest_job.to_job(
    name="ingest_job_local",
    config={**dev_local, **db_config},
    resource_defs={"database": postgres_resource},
    tags={"dev": True},
)


create_date_dim_local = dates_job.to_job(
    name="create_date_dim_local",
    config=db_config,
    resource_defs={"database": postgres_resource},
    tags={"dev": True},
)


## Alternative way
# @job(name="download_job_local", tags={"dev": True})
# def download_job():
#     # download dataset
#     download_dataset()


# @job(name="ingest_job_local",
#     config={**dev_local, **db_config},
#     resource_defs={"database": postgres_resource},
#     tags={"dev": True},)
# def ingest_job():
#     ingest_datasets_raw()


# @job(name="create_date_dim_local",
#     config=db_config,
#     resource_defs={"database": postgres_resource},
#     tags={"dev": True})
# def dates_job():
#     create_dim_dates()