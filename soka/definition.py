from dagster import Definitions
from soka.jobs.job import download_job_local, ingest_job_local, create_date_dim_local
from soka.sensors.sensor import download_ingest_sensor
from soka.assets.asset import appearances, club_games, clubs, competitions, game_events, games, player_valuations, players #view_asset_tags


defs = Definitions(
    assets=[appearances, club_games, clubs, competitions, game_events, games, player_valuations, players],
    sensors=[download_ingest_sensor],
    jobs=[download_job_local, ingest_job_local, create_date_dim_local]
)