{{ config(
    schema='prod',
    materialized='table'

) }}

WITH date_key_info AS (
    SELECT * FROM {{ref("stg_date_key")}}
),
match_key_info AS (
    SELECT * FROM {{ref("stg_match_key")}}
),
player_key_info AS (
    SELECT * FROM {{ref("stg_player_key")}}
),
merged_events AS (SELECT
    game_event_id, 
    game_id, 
    player_id, 
    date, 
    minute, 
    type,
    description
FROM {{ref("stg_adjusted_substitutions")}}
UNION ALL
SELECT
    game_event_id, 
    game_id, 
    player_id, 
    date, 
    minute, 
    type,
    description
FROM {{ref("stg_substitutions_in")}}
UNION ALL
SELECT 
	game_event_id, 
	game_id, 
	player_id, 
	date, 
	minute, 
	type,
	description
FROM {{ref("stg_assisters")}}),
final AS (
SELECT
    match_key, 
	player_key, 
	date_key, 
    game_event_id,
	minute, 
	type,
	description
    FROM merged_events
    LEFT JOIN date_key_info ON merged_events.date = date_key_info.date
    LEFT JOIN match_key_info ON merged_events.game_id = match_key_info.game_id
    LEFT JOIN player_key_info ON merged_events.player_id = player_key_info.player_id
)
SELECT * FROM final