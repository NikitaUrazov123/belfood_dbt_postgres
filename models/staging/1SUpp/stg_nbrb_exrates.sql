{{ config(
    materialized='incremental'
) }}

with 
source as(
SELECT *
FROM {{ source('analytics_shipments', 'nbrb_exrates') }}),

types as 
(
  select 
  "Cur_ID",
	toDate("Date") as date,
	"Cur_Abbreviation",
	"Cur_Scale",
	"Cur_Name",
	"Cur_OfficialRate"
  from source
),

signed as
(
    select
    *,
    now() as update_at
    from types
)

select * from signed

{% if is_incremental() %}
  where date >= dateAdd(day, -10, today())
{% endif %}