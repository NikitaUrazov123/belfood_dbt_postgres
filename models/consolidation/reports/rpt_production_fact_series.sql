{{ config(
    materialized='view'
) }}

with 
base as
(
    select * from 
    {{ ref('mrt_production_fact_series') }}
)

SELECT * 
FROM base
