from dagster import graph
from soka.ops.op import download_dataset, ingest_datasets
from soka.resources.resource import postgres_resource
from soka.core.config import settings

index_cols = {
    "appearances": ["appearance_id"], 
    "club_games": ["club_id", "game_id"], 
    "clubs": ["club_id"], 
    "competitions": ["competition_id"], 
    "game_events": ["game_id", "club_id", "player_id", "minute"], 
    "games": ["game_id"], 
    "player_valuations": ["date", "player_id"], 
    "players": ["player_id"]}

dev_local = {
    "resources": {
        "database": {
            "config": {
                "host": settings.db_host,
                "port": settings.db_port,
                "user": settings.db_username,
                "password": settings.db_password,
                "database": settings.db_name
            }
        }
    },
    "ops": {"ingest_datasets": {"config": {"index_cols": index_cols, "dir": "./data"}}},
}


@graph
def download_job():
    # download dataset
    download_dataset()
        

@graph
def ingest_job():
    ingest_datasets()


download_job_local = download_job.to_job(name="download_job_local", 
                                 tags={"dev": True})

ingest_job_local = ingest_job.to_job(name="ingest_job_local", 
                                 config=dev_local,
                                 resource_defs={"database": postgres_resource},
                                 tags={"dev": True})

