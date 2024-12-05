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
),

filtred2 as
(
    SELECT * 
    FROM filtred
    WHERE
        COALESCE("Категория направления", '') IN ('Беларусь', 'e-commerce РБ')
        AND COALESCE("Категория направления", '') NOT LIKE '%pack%'
        AND COALESCE("Регион торгового объекта", '') NOT IN ('Монголия')
        AND COALESCE("Тип канала контрагента", '') != 'Экспорт'
)

SELECT * 
FROM filtred2
