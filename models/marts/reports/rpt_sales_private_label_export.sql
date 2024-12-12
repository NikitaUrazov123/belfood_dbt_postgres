with 
filtred as
(
    SELECT * 
    FROM {{ ref("mrt_sales") }}
    WHERE
        "Вид производства номенклат." = 'СТМ'
        and
        "Код страны контрагента" <> 'BLR'
)
SELECT * 
FROM filtred
