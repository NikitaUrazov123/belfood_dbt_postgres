{{ config(
    materialized='view',
    tags=["subdim"]
) }}

SELECT
    `Штрихкод`, `ВладелецГуид`
FROM
    {{ source('Stage1CUpp', 'РС_Штрихкоды') }}