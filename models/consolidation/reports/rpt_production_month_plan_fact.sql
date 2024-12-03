{{ config(
    materialized='view'
) }}

with 
base as
(
    select * from 
    {{ ref('mrt_production_month_plan_fact') }}
)

SELECT * 
FROM base
