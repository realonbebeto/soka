{{ config(
    schema='prod',
    materialized='table'

) }}

WITH clubs as (
    SELECT
    club_id,
    club_name
    FROM {{ref('stg_club_names')}}
), 
games as (
SELECT 
    game_id, 
    season,
    round,
    date,
    home_club_id,
    away_club_id,
    home_club_manager_name,
    away_club_manager_name,
    competition_id,
    home_club_goals,
    away_club_goals,
    stadium,
    attendance,
    referee,
    home_club_formation,
    away_club_formation
FROM {{source('soka_raw', 'game')}}),
result as (
    SELECT
    {{ dbt_utils.generate_surrogate_key(['games.game_id', 'games.date']) }} as match_key,
    games.game_id,
    games.season,
    games.round,
    games.date,
    a.club_name as home_club_name,
    b.club_name as away_club_name,
    games.home_club_manager_name,
    games.away_club_manager_name,
    c.competition_name,
    c.competition_sub_type,
    c.competion_type,
    c.competition_country,
    c.competition_confederation,
    games.home_club_goals,
    games.away_club_goals,
    games.stadium,
    games.attendance,
    games.referee,
    games.home_club_formation,
    games.away_club_formation
    FROM games
    LEFT JOIN clubs a on games.home_club_id = a.club_id
    LEFT JOIN clubs b on games.away_club_id = b.club_id
    LEFT JOIN {{ref("stg_confederation_details")}} c on games.competition_id = c.competition_id
)
SELECT * FROM result



