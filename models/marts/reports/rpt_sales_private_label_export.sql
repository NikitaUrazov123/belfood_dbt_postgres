{{ config(
    materialized='view'
) }}

with 
filtred as
(
    SELECT * 
    FROM {{ ref("mrt_sales") }}
    WHERE
        "Направление продукта номенклат." = 'СТМ'
        and
        "Код страны контрагента" <> 'BLR'
)
SELECT * 
FROM filtred
