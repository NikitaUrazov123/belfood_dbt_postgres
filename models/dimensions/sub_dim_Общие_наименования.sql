{{ config(
    materialized='view',
    tags=["sub_dim"]
) }}

SELECT
    `ОбъектГуид`, `Значение`
FROM
    {{ ref('stg_РС_ЗначенияСвойствОбъектов') }}
WHERE
    `Свойство` = 'Общее наименование'