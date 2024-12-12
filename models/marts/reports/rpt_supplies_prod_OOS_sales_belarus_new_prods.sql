with 
rpt_supplies_prod_OOS as 
(
SELECT * 
FROM {{ ref("rpt_supplies_prod_OOS") }}
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
    from rpt_supplies_prod_OOS
    left join saled_belarus_new_prods on 
    rpt_supplies_prod_OOS."Гуид номенклатуры" = saled_belarus_new_prods."НоменклатураГуид"
    where "Новинка за последние 6 месяцев?" = true
)

select * from filtred
