{{ config(
    materialized='view',
    tags=["subdim"]
) }}

SELECT
    `ОбъектГуид`, `Значение`
FROM
    {{ ref('stg_РС_ЗначенияСвойствОбъектов') }}
WHERE
    `Свойство` = 'Вид ДП'