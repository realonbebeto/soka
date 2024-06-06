{{ config(materialized='view') }}


SELECT 
    game_event_id, 
    game_id,
    player_in_id as player_id,
    date,
    minute, 
    case
        when type = 'Substitutions' then 'Substitution In'
        else type
    end as type, 
    description
    FROM {{source("soka_raw", "game_event")}}
    WHERE player_in_id IS NOT NULL