{{ config(
    materialized='view'
) }}

with source as (
      select * from {{ source('Stage1CUpp', 'С_НоменклатурныеГруппы') }}
)
select * from source
  