{{ config(
    materialized='table',
) }}

with 
base as
(
    select * from {{ ref("exp_production_fact_series") }}
),

filtred as
(
    select * from base
    where "Вид номенклат." != 'Материалы'
),

defined_props as 
(
    select 
    *
    from filtred
)

select * from defined_props