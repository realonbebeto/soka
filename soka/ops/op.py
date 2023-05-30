import kaggle
from dagster import op, AssetMaterialization, Output, Field, RetryPolicy, DataVersion
from soka.core.config import settings
from soka.assets.asset import ins
from typing import Union
from datetime import datetime
import os
from soka.utils.util import convert_dataversion_to_int


dataset_id = settings.dataset_id

def reset_temp_dir(dir: str):
    for csv in os.listdir(dir):
        if csv.endswith(".csv"):
            os.remove(os.path.join(dir, csv))

#Op that fetches the latest version of data to be used to trigger a sensor
@op(name="latest_version", retry_policy=RetryPolicy(max_retries=3, delay=3), description="Fetching the version and logging it as an output")
def fetch_version():
    kaggle.api.authenticate()
    versions = list(kaggle.api.dataset_view(dataset_id).versions)
    version = convert_dataversion_to_int(versions[0])
    yield AssetMaterialization(asset_key="latest_version",
                               description="Fetched version",
                               metadata={"version": version})
    yield Output(version, data_version=DataVersion(str(version)))


@op(description="Downloading kaggle dataset")
def download_dataset(context) -> Union[int, None]:
    try:
        # authenticate 
        kaggle.api.authenticate()
        kaggle.api.dataset_download_files(dataset_id, path='./data', unzip=True)
        version = convert_dataversion_to_int(list(kaggle.api.dataset_view(dataset_id).versions)[0])
        context.log.info(f"New dataset downloaded successfully")
        return version
    except Exception as err:
        context.log.error(f"Error downloading dataset: {err}")


@op(config_schema={"index_cols": Field(dict), "dir": Field(str)}, required_resource_keys={"database"}, description="Ingesting the csv to postgres")
def ingest_datasets(context):
    index_cols = context.op_config["index_cols"]
    dir = context.op_config["dir"]
    metadata = {}
    for asset in ins.keys():
        # Run the asset function
        data = ins[asset]()
        table_name = asset[:-1]
        number_of_rows = data.shape[0]
        context.resources.database.ingest_data(table_name, data, index_cols[asset])
        context.log.info(f"New {asset} {number_of_rows} records ingested to {table_name} successfully")
        metadata[asset] = {"table_name": table_name, "number_of_rows": number_of_rows}

    asset_key = asset + f"{datetime.utcnow()}"
    yield AssetMaterialization(asset_key=asset_key,
                               description="Inserting a random batch of records",
                               metadata=metadata)
    yield Output(metadata)
    
    #Clearing the temporary data directory
    reset_temp_dir(dir)
    context.log.info("Data dir reset/cleared")