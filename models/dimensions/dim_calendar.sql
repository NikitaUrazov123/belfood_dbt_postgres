{{ config(
    materialized='view'
) }}

with source as (
      select * from {{ source('analytics_shipments', 'calendar2') }}
)

select * from source
  