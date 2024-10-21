{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source('Stage1CUpp', 'РС_Штрихкоды') }}
