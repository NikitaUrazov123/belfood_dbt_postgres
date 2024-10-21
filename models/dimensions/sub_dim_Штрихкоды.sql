{{ config(
    materialized='view',
    tags=["subdim"]
) }}

SELECT
    `Штрихкод`, `ВладелецГуид`
FROM
    {{ ref('stg_РС_Штрихкоды') }}