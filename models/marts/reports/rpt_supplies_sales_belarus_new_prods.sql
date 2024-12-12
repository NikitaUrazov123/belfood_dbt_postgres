with 
mrt_prod_supplies as 
(
SELECT * 
FROM {{ ref("mrt_prod_supplies_2") }}
),

saled_belarus_new_prods as
(
    select distinct "НоменклатураГуид", "Новинка за последние 6 месяцев?"
    from {{ ref("rpt_sales_belarus_new_prods") }}
),

filtred as
(
    select
    *
    from mrt_prod_supplies
    left join saled_belarus_new_prods on 
    mrt_prod_supplies."Гуид номенклатуры" = saled_belarus_new_prods."НоменклатураГуид"
    where "Новинка за последние 6 месяцев?" = true
)

select * from filtred
