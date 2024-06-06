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
final AS (
    SELECT 
        match_key_info.match_key,
        player_key_info.player_key,
        b.club_key,
        date_key_info.date_key,
        game_lineups_id,
        type,
        position,
        team_captain,
        c.minutes_played
    FROM {{source("soka_raw", "game_lineup")}} a
    LEFT JOIN date_key_info ON a.date = date_key_info.date
    LEFT JOIN match_key_info ON a.game_id = match_key_info.game_id
    LEFT JOIN player_key_info ON a.player_id = player_key_info.player_id
    LEFT JOIN {{ref("club_dim")}} b ON a.club_id = b.club_id
    LEFT JOIN {{source("soka_raw", "appearance")}} c ON a.game_id = c.game_id
    AND a.player_id = c.player_id AND a.date = c.date
)

SELECT * FROM final
