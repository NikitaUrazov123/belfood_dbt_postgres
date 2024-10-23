{{ config(
    materialized='view'
) }}

with source as (
      select * from {{ source('Stage1CUpp', 'С_ТорговыеОбъекты') }}
)
select * from source
  