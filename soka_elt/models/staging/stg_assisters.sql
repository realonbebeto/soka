{{ config(materialized='view') }}

SELECT 
    game_event_id, 
    game_id, 
    player_assist_id as player_id, 
    date, 
    minute, 
    type,
    description
FROM {{source("soka_raw", "game_event")}}
WHERE player_assist_id IS NOT NULL