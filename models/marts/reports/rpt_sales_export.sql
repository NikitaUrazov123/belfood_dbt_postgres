with 
filtred as
(
    SELECT * 
    FROM {{ ref("mrt_sales") }}
    WHERE "Код страны контрагента" <> 'BLR'
)
SELECT * 
FROM filtred
