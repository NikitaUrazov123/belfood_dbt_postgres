{{ config(
    materialized='view',
    tags=["dim"]
) }}

with source as (
      select * from {{ source('Stage1CUpp', 'Д_ВозвратТоваровОтПокупателя') }}
)

select * from source