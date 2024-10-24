{{ config(
    materialized='view',
    tags=["dim"]
) }}

with source as (
      select * from {{ source('Stage1CUpp', 'С_ТорговыеОбъекты') }}
)
select * from source