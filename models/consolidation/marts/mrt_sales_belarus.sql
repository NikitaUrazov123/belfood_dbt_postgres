{{ config(
    materialized='view'
) }}

SELECT * from {{ ref("mrt_sales") }}
WHERE
"Категория направления" in ('Беларусь', 'e-commerce РБ')
and
"Категория направления" not like '%pack%'
and "Регион торгового объекта" not in ('Монголия')
and "Тип канала контрагента" not in ('Экспорт')