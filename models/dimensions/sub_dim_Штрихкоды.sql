{{ config(
    materialized='view',
    tags=["sub_dim"]
) }}

SELECT
    `Штрихкод`, `ВладелецГуид`
FROM
    {{ ref('stg_РС_Штрихкоды') }}