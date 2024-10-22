{{
    config(
        materialized='incremental'
    )
}}


SELECT *,
concat(toString(`НомерСтроки`),toString(`РегистраторГуид`)) as unique_key, 
now() as updated_at FROM {{ ref('stg_РН_Продажи') }}

{% if is_incremental() %}
  where "Период" >= dateAdd(month, -3, today())
{% endif %}
