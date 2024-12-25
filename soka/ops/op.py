from datetime import datetime, timedelta

import kaggle
from dagster import AssetMaterialization, DataVersion, Field, Output, RetryPolicy, op

from soka.assets.asset import ins
from soka.core.config import settings
from soka.utils.util import dimension_dates, reset_temp_dir

dataset_id = settings.dataset_id


# Op that fetches the latest version of data to be used to trigger a sensor
@op(
    config_schema={"version": Field(int)},
    name="record_version",
    retry_policy=RetryPolicy(max_retries=3, delay=3),
    description="Fetching the version and logging it as an output",
)
def record_version(context):
    version = context.op_config["version"]
    context.log.info(f"{version} recorded")
    yield AssetMaterialization(
        asset_key="latest_version",
        description="New fetched version",
        metadata={"curr_version": version},
    )
    yield Output(version, data_version=DataVersion(str(version)))


@op(description="Downloading kaggle dataset")
def download_dataset(context) -> None:
    try:
        # authenticate
        kaggle.api.authenticate()
        kaggle.api.dataset_download_files(dataset_id, path="./data", unzip=True)
        # version = convert_dataversion_to_int(list(kaggle.api.dataset_view(dataset_id).versions)[0])
        context.log.info("New dataset downloaded successfully")
        # return version
    except Exception as err:
        context.log.error(f"Error downloading dataset: {err}")


@op(
    config_schema={"dir": Field(str)},
    required_resource_keys={"database"},
    description="Ingesting the csv to postgres",
)
def ingest_datasets_raw(context):
    dir = context.op_config["dir"]
    metadata = {}
    for asset in ins.keys():
        # Run the asset function
        data = ins[asset]["asset"]()
        primary_cols = ins[asset]["primary_cols"]
        table_name = asset[:-1]
        

        existing_ids = context.resources.database.get_existing_records("raw", table_name, primary_cols)
        data["record_key"] = data[primary_cols].astype(str).agg("_".join, axis=1)

        new_records = data[~data["record_key"].isin(existing_ids)].copy().drop(columns=["record_key"])
        number_of_rows = new_records.shape[0]
        context.resources.database.ingest_data("raw", table_name, new_records, "append")
        context.log.info(
            f"New {asset} {number_of_rows} records ingested to {table_name} successfully"
        )
        metadata[asset] = {"table_name": table_name, "number_of_rows": number_of_rows}

    # reset_temp_dir(dir)
    asset_key = asset + f"{datetime.utcnow()}"
    yield AssetMaterialization(
        asset_key=asset_key,
        description="Inserting a couple batch of records",
        metadata=metadata,
    )
    yield Output(metadata)

    # Clearing the temporary data directory
    # reset_temp_dir(dir)
    # context.log.info("Data dir reset/cleared")


@op(required_resource_keys={"database"}, description="Ingesting the date dim")
def create_dim_dates(context):
    start_date = context.resources.database.get_min_date("raw", "appearance")
    start_date = datetime.strftime((start_date - timedelta(365)), "%Y-%m-%d")
    end_date = datetime.today().strftime("%Y-%m-%d")

    all_dates = dimension_dates(start_date, end_date)

    context.resources.database.ingest_data("prod", "date_dim", all_dates)
    context.log.info(
        f"date_dim {all_dates.shape[0]} records ingested to prod.date_dim successfully"
    )


@op(required_resource_keys={"database"}, description="Ingesting the csv to postgres")
def init_db(context):
    start_date = context.resources.database.get_min_date("raw", "appearance")

    context.log.info("init db run successfully")
