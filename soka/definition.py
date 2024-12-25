from dagster import Definitions

from soka.assets.asset import (
    appearances,
    club_games,
    clubs,
    competitions,
    game_events,
    games,
    player_valuations,
    players,
)  # view_asset_tags
from soka.jobs.job import create_date_dim_local, download_job_local, ingest_job_local
from soka.sensors.sensor import check_ingest_files, check_raw_files

defs = Definitions(
    assets=[
        appearances,
        club_games,
        clubs,
        competitions,
        game_events,
        games,
        player_valuations,
        players,
    ],
    sensors=[check_ingest_files, check_raw_files],
    jobs=[download_job_local, ingest_job_local, create_date_dim_local],
)
