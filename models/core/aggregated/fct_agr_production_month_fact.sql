with
base as
(
select * from {{ ref('fct_production_fact') }}
),

agregated as
(
    select
    date_trunc('month',"Период выпуска прод.")::date as "Месяц выпуска прод."
    ,"Подразделение выпуска прод."
    ,"ПродукцияГуид"
    ,sum("Количество выпуска прод.") as "Количество выпуска прод."
    from base
    group by 1,2,3
),

defined_props as 
(
    SELECT 
    *
    ,{{ dbt_utils.generate_surrogate_key(['\"Месяц выпуска прод.\"', "\"ПродукцияГуид\""]) }} as month_prod_key
    FROM agregated
)

select * from defined_props