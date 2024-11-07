{{ config(
    materialized='incremental'
) }}

with
source as 
(
    select * from {{ source('Stage1CUpp', 'Д_РеализацияТоваровУслуг') }}),

filtred as 
(
    select * from source
    where "ПометкаУдаления" = False
)
select * from filtred

{% if is_incremental() %}
  where "Дата"::date >= CURRENT_DATE - interval '2 months'
{% endif %}