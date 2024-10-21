{{ config(
    materialized='view'
) }}

SELECT
    `Штрихкод`, `ВладелецГуид`
FROM
    {{ ref('stg_РС_Штрихкоды') }}