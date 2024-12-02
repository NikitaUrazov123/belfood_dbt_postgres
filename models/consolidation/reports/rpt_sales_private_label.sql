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
)
SELECT * 
FROM filtred
