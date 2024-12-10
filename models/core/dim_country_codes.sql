{{ config(
    materialized='view'
) }}

with 
source as (
select * from {{ ref("stg_refs__country_codes_alpha") }}
)

select * from source
  