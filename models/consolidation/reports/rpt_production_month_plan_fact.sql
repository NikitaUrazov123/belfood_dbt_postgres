{{ config(
    materialized='view'
) }}

with 
base as
(
    select * from 
    {{ ref('mrt_production_month_plan_fact') }}
),

current_month_progress as
(
    select * from base
    cross join {{ ref("current_month_passing") }}
)

SELECT * 
FROM current_month_progress
