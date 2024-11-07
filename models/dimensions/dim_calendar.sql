{{ config(
    materialized='table',
    tags=["no_update"]
) }}

with source as (
      select * from {{ source('refs', 'calendar2') }}
)

select * from source
  