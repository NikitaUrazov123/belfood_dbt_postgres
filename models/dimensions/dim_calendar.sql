{{ config(
    materialized='view',
    tags=["no_update"]
) }}

with source as (
      select * from {{ source('refs', 'calendar2') }}
)

select * from source
  