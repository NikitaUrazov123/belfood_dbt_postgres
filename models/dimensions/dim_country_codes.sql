with 
source as (
select * from {{ source('refs', 'country_codes_alpha') }}
)

select * from source
  