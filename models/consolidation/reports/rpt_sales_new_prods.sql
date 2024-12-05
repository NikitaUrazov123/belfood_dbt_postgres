{{ config(
    materialized='view'
) }}

with 
filtred as
(
    SELECT * 
    FROM {{ ref("mrt_sales") }}
    WHERE
    "Дата первой продажи по артикулу" ::date >= CURRENT_DATE - interval '6 months'
    or "Статус номенклат." ='Новинка'
)

SELECT * 
FROM filtred
