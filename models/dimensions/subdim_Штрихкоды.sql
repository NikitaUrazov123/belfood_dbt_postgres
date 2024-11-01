{{ config(
    tags=["subdim"]
) }}

SELECT
    `Штрихкод`, `ВладелецГуид`
FROM
    {{ source('Stage1CUpp', 'РС_Штрихкоды') }}