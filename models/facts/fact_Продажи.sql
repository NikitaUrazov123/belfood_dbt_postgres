{{ config(
    materialized='table'
) }}

SELECT *,
concat(toString(`НомерСтроки`),toString(`РегистраторГуид`)) as unique_key, 
now() as updated_at FROM {{ ref('stg_РН_Продажи') }}
