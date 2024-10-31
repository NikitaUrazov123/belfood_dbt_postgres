{{ config(
    materialized='table'
) }}

SELECT * FROM {{ source('Stage1CUpp', 'РС_ЗначенияСвойствОбъектов') }}
