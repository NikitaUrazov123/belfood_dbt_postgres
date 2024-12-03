{{ config(
    materialized='table',
) }}

with 
base as
(
    select * from {{ ref("exp_production_month_plan_fact") }}
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
    ,case
        when "План производства, шт" - "Факт производства, шт" > 0 then "План производства, шт" - "Факт производства, шт"
        else 0
    end as "Остаток производства, шт"
    from filtred
)

select * from defined_props