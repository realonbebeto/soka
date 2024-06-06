{{ config(materialized='view') }}

SELECT 
competition_id, 
name as competition_name,
type as competion_type,
sub_type as competition_sub_type,
country_name as competition_country,
confederation as competition_confederation
FROM {{source('soka_raw', 'competition')}}