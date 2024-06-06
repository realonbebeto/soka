{{ config(
    schema='prod',
    materialized='table'

) }}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['player_id']) }} as player_key,
    player_id, 
    first_name, 
    last_name,
    name as full_name,
    last_season,
    country_of_birth,
    city_of_birth,
    country_of_citizenship,
    date_of_birth,
    foot,
    height_in_cm,
    agent_name
FROM {{source('soka_raw', 'player')}}