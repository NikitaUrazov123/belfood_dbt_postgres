{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source('Stage1CUpp', 'С_СерииНоменклатуры') }}
