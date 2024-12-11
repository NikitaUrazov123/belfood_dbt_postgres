with 
today_supplies as
(
    select * 
    from {{ ref("mrt_prod_supplies") }}
    where "Дата остатков"::date = CURRENT_DATE::date
)
select * from today_supplies