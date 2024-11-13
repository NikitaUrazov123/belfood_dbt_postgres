{{ config(
    materialized='incremental',
    unique_key='key_record',
    tags=["incremental"]
) }}

with
source as
(
    select * from {{ source('Stage1CUpp', 'РН_ЗаказыПокупателей') }}
),

signed as 
(
    SELECT 
    *
    ,concat("РегистраторГуид", "НомерСтроки") as key_record
    ,now() as updated_at
    FROM source
)

select * from signed

{% if is_incremental() %}
  where "Период"::date >= CURRENT_DATE - interval '2 months'
{% endif %}
