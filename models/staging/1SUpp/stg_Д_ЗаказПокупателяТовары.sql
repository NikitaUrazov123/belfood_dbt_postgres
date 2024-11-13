{{ config(
    materialized='incremental',
    unique_key='key_record',
    tags=["incremental"]
) }}

with
source as
(
    select * from {{ source('Stage1CUpp', 'Д_ЗаказПокупателяТовары') }}
),

signed as 
(
    SELECT 
    *
    ,concat("СсылкаГуид", "НомерСтроки") as key_record
    ,now() as updated_at
    FROM source
)

select * from signed

{% if is_incremental() %}
  where "Дата"::date >= CURRENT_DATE - interval '2 months'
{% endif %}
