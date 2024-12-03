{{
    config(
        materialized='view'
    )
}}

with
base as
(
select
{{star_exclude_guid(ref('fct_production_month_plan'), additional_excludes=["key_record"])}}
,"НоменклатураГуид"
from {{ ref('fct_production_month_plan') }}
),

agregated as
(
    select
    date_trunc('month', "Период мес. плана производ.") as  "Месяц мес. плана производ."
    ,"Подразделение мес. плана производ."
    ,"Номенклатура мес. плана производ."
    ,"НоменклатураГуид"
    ,sum("Количество мес. плана производ.") as "Количество мес. плана производ."
    from base
    group by 1,2,3,4
),

signed as
(
    select 
    *
    ,"Месяц мес. плана производ."::text||"НоменклатураГуид" as key_month_prod
    from agregated
)

select * from signed