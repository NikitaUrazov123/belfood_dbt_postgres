{{
    config(
        materialized='incremental'
    )
}}

SELECT *, now() as updated_at FROM {{ ref('stg_РН_Продажи') }}

{% if is_incremental() %}
  where "Период" >= dateAdd(month, -2, today())
{% endif %}
