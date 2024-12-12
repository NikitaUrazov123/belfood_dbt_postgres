with 
filtred as
(
    SELECT * 
    FROM {{ ref("mrt_sales") }}
    WHERE
    "Вид производства номенклат." = 'Товары покупные'
)

SELECT * 
FROM filtred
