{{ config(
    materialized='view',
    tags=["subdim"]
) }}

SELECT
    `ОбъектГуид`, `Значение`
FROM
    {{ ref('stg_РС_ЗначенияСвойствОбъектов') }}
WHERE
    `Свойство` = 'Группа товаров для отчетов'