import kaggle
from soka.core.config import settings
from dagster import asset, RetryPolicy, AssetMaterialization, Output
import pandas as pd
from datetime import datetime


dataset_id = settings.dataset_id

def convert_DataVersion_to_int(dv):
    return int(dv.versionNumber)

# Asset that fetches the latest version of data to be used to trigger a sensor
@asset(name="latest_version", compute_kind="data_poll", retry_policy=RetryPolicy(max_retries=3, delay=3))
def fetch_versions() -> int:
    kaggle.api.authenticate()
    versions = list(kaggle.api.dataset_view(dataset_id).versions)
    asset_key = f"version_{datetime.utcnow()}"
    metadata = {"latest_version": convert_DataVersion_to_int(versions[0])}
    yield AssetMaterialization(asset_key=asset_key, metadata=metadata)
    version = convert_DataVersion_to_int(versions[0])
    # yield Output(version)
    return version


@asset(compute_kind="read_data")
def appearances():
    return pd.read_csv("./data/appearances.csv").astype({'date': 'datetime64[ns]'})

@asset(compute_kind="read_data")
def club_games():
    return pd.read_csv("./data/club_games.csv")

@asset(compute_kind="read_data")
def clubs():
    return pd.read_csv("./data/clubs.csv")

@asset(compute_kind="read_data")
def competitions():
    return pd.read_csv("./data/competitions.csv")

@asset(compute_kind="read_data")
def game_events():
    return pd.read_csv("./data/game_events.csv")

@asset(compute_kind="read_data")
def games():
    return pd.read_csv("./data/games.csv").astype({'date': 'datetime64[ns]'})

@asset(compute_kind="read_data")
def player_valuations():
    return pd.read_csv("./data/player_valuations.csv").astype({'date': 'datetime64[ns]', 'datetime': 'datetime64[ns]', 'dateweek': 'datetime64[ns]'})

@asset(compute_kind="read_data")
def players():
    return pd.read_csv("./data/players.csv")

ins = {
    "appearances":appearances, 
    "club_games": club_games, 
    "clubs": clubs, 
    "competitions": competitions, 
    "game_events": game_events, 
    "games": games, 
    "player_valuations": player_valuations, 
    "players": players}