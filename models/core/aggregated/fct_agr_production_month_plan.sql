with
base as
(
select * from {{ ref('fct_production_month_plan') }}
),

agregated as
(
    select
    date_trunc('month', "Период плана производ.")::date as  "Месяц плана производ."
    ,"Подразделение плана производ."
    ,"НоменклатураГуид"
    ,sum("Количество плана производ.") as "Количеств плана производ."
    from base
    group by 1,2,3
),

defined_props as 
(
    SELECT 
    *
    ,{{ dbt_utils.generate_surrogate_key(['\"Месяц плана производ.\"', "\"НоменклатураГуид\""]) }} as month_prod_key
    FROM agregated
)

select * from defined_props