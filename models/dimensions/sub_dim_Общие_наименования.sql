{{ config(
    materialized='view'
) }}

SELECT
    `ОбъектГуид`, `Значение`
FROM
    {{ ref('stg_РС_ЗначенияСвойствОбъектов') }}
WHERE
    `Свойство` = 'Общее наименование'