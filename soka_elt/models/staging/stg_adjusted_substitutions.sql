{{ config(materialized='view') }}

SELECT
	game_event_id, 
    game_id, 
    player_id,
    date, 
    minute,
	case
		when type = 'Substitutions' then 'Substitution Out'
		else type
	end as type,
	description
FROM {{source("soka_raw", "game_event")}}