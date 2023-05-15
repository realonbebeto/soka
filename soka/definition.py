from dagster import Definitions
from soka.jobs.job import download_job_local, ingest_job_local
from soka.sensors.sensor import latest_version_sensor, check_files_sensor
from soka.assets.asset import fetch_version, appearances, club_games, clubs, competitions, game_events, games, player_valuations, players


defs = Definitions(
    assets=[fetch_version, appearances, club_games, clubs, competitions, game_events, games, player_valuations, players],
    sensors=[latest_version_sensor, check_files_sensor],
    jobs=[download_job_local, ingest_job_local]
)