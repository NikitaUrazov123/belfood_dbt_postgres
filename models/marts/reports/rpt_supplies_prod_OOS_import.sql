with 
rpt_supplies_prod_OOS as 
(
SELECT * 
FROM {{ ref("rpt_supplies_prod_OOS") }}
),

filtred as
(
    select * from rpt_supplies_prod_OOS
    where "Вид производства номенклат." = 'Товары покупные'
)

select * from filtred
