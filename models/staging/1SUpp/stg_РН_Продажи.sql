{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source('Stage1CUpp', 'РН_Продажи') }}
