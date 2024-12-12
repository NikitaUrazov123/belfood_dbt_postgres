with 
mrt_prod_supplies_import as
(
    select * 
    from {{ ref("mrt_prod_supplies_2") }}
    WHERE
    "Вид производства номенклат." = 'Товары покупные'
)
select * from mrt_prod_supplies_import