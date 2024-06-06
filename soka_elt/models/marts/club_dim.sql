{{ config(
    schema='prod',
    materialized='table'

) }}

SELECT 
    {{ dbt_utils.dbt_utils.generate_surrogate_key(['club_id']) }} as club_key,
    club_id, 
    name as club_name, 
    stadium_name,
    stadium_seats
FROM {{source('soka_raw', 'club')}}