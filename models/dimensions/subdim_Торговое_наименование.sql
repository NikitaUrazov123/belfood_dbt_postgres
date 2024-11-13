{{ config(
    materialized='view',
    tags=["subdim"]
) }}

SELECT
    "ОбъектГуид", "Значение"
FROM
    {{ ref('stg_РС_ЗначенияСвойствОбъектов') }}
WHERE
    "Свойство" = 'Торговое наименование (если сеть)'