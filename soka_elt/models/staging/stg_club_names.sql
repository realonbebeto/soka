{{ config(materialized='view') }}

SELECT club_id, name as club_name
FROM {{source('soka_raw', 'club')}}