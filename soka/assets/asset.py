from dagster import asset
import pandas as pd


@asset(compute_kind="read_data")
def appearances():
    df = pd.read_csv("./data/appearances.csv")
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    return df

@asset(compute_kind="read_data")
def club_games():
    df = pd.read_csv("./data/club_games.csv")
    df.dropna(subset=['club_id', 'opponent_id'], inplace=True)
    return df

@asset(compute_kind="read_data")
def clubs():
    return pd.read_csv("./data/clubs.csv")

@asset(compute_kind="read_data")
def competitions():
    return pd.read_csv("./data/competitions.csv")

@asset(compute_kind="read_data")
def game_events():
    df = pd.read_csv("./data/game_events.csv")
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    return df

@asset(compute_kind="read_data")
def game_lineups():
    df = pd.read_csv("./data/game_lineups.csv")
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    return df

@asset(compute_kind="read_data")
def games():
    df = pd.read_csv("./data/games.csv")
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    return df

@asset(compute_kind="read_data")
def player_valuations():
    df = pd.read_csv("./data/player_valuations.csv")
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    return df

@asset(compute_kind="read_data")
def players():
    df = pd.read_csv("./data/players.csv")
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth']).dt.strftime('%Y-%m-%d')
    return df

# @asset(name="view_asset_tags")
# def view_asset_tags(context):
#     print(context.instance.get_event_records(
# EventRecordsFilter(asset_key=AssetKey("latest_version"),
#         event_type=DagsterEventType.ASSET_MATERIALIZATION)
#     )[-1].event_log_entry.dagster_event.event_specific_data.materialization.metadata["version"].value)

ins = {
    "appearances": {"asset": appearances, "primary_cols": ["appearance_id"]}, 
    "club_games": {"asset": club_games, "primary_cols": ["game_id", "club_id", "opponent_id"]}, 
    "clubs": {"asset": clubs, "primary_cols": ["club_id"]}, 
    "competitions": {"asset": competitions, "primary_cols": ["competition_id"]}, 
    "game_events": {"asset": game_events, "primary_cols": ["game_event_id"]}, 
    "game_lineups": {"asset": game_lineups, "primary_cols": ["game_lineups_id"]},
    "games": {"asset": games, "primary_cols": ["game_id"]}, 
    "player_valuations": {"asset": player_valuations, "primary_cols": ["player_id", "date"]}, 
    "players": {"asset": players, "primary_cols": ["player_id"]}}