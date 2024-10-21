{{ config(
    materialized='table'
) }}

SELECT * FROM {{ ref('stg_РН_Продажи') }}
