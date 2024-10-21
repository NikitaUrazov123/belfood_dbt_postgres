{{
    config(
        materialized='incremental'
    )
}}

select
    *
from {{ ref("stg_РН_ТоварыНаСкладахОстатки") }}

{% if is_incremental() %}
  where "ДатаОстатка" >= dateAdd(day, -1, today())
{% endif %}
