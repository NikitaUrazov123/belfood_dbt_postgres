{{ config(
    materialized='table'
) }}

SELECT * FROM {{ source('Stage1CUpp', 'РН_Продажи') }}
