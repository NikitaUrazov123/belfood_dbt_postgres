{{
    config(
        materialized='view'
    )
}}

with
base as
(
select
{{star_exclude_guid(ref('fct_production_fact'))}}
,"ПродукцияГуид"
from {{ ref('fct_production_fact') }}
),

agregated as
(
    select
    date_trunc('month',"Период выпуска прод.") as "Месяц выпуска прод."
    ,"Подразделение выпуска прод."
    ,"Продукция выпуска прод."
    ,"ПродукцияГуид"
    ,sum("Количество выпуска прод.") as "Количество выпуска прод."
    from base
    group by 1,2,3,4
),

signed as 
(
    select
    *
    ,"Месяц выпуска прод."::text||"ПродукцияГуид" as key_month_prod
    from agregated
)

select * from signed