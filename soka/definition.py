from dagster import Definitions
from soka.jobs.job import version_job_local, download_job_local, ingest_job_local
from soka.sensors.sensor import latest_version_sensor, check_files_sensor
from soka.assets.asset import appearances, club_games, clubs, competitions, game_events, games, player_valuations, players #view_asset_tags


defs = Definitions(
    assets=[appearances, club_games, clubs, competitions, game_events, games, player_valuations, players],
    sensors=[latest_version_sensor, check_files_sensor],
    jobs=[version_job_local, download_job_local, ingest_job_local]
)