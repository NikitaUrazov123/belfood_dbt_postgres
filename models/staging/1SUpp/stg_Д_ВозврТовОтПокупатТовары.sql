{{ config(
    materialized='incremental',
    unique_key='key_record'
) }}

with
source as 
(
    select * from {{ source('Stage1CUpp', 'Д_ВозврТовОтПокупатТовары') }}),

signed as
(
  select
  *
  ,concat("СсылкаГуид", "НомерСтроки") as key_record
  ,now() as updated_at
  FROM source
)
    
select * from signed

{% if is_incremental() %}
  where "Дата"::date >= CURRENT_DATE - interval '2 months'
{% endif %}
