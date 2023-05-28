from dagster import asset, AssetKey, DagsterEventType, EventRecordsFilter
import pandas as pd


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

# @asset(name="view_asset_tags")
# def view_asset_tags(context):
#     print(context.instance.get_event_records(
# EventRecordsFilter(asset_key=AssetKey("latest_version"),
#         event_type=DagsterEventType.ASSET_MATERIALIZATION)
#     )[-1].event_log_entry.dagster_event.event_specific_data.materialization.metadata["version"].value)

ins = {
    "appearances":appearances, 
    "club_games": club_games, 
    "clubs": clubs, 
    "competitions": competitions, 
    "game_events": game_events, 
    "games": games, 
    "player_valuations": player_valuations, 
    "players": players}