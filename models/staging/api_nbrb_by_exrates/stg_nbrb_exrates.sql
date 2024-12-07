{{ config(
    materialized='incremental',
    unique_key='key_record',
    tags=["incremental"]
) }}

with 
source as(
SELECT *
FROM {{ source('api_nbrb_by_exrates', 'rates') }}),

types as 
(
  select 
  "Cur_ID"::text as cur_id,
	"Date"::date as date,
	"Cur_Abbreviation" as Cur_Abbreviation,
	"Cur_Scale" as Cur_Scale,
	"Cur_OfficialRate" as Cur_Official_Rate
  from source
),

signed as
(
    select
    *
    ,concat(to_char(date, 'DDMMYYYY'),cur_id) as key_record
    ,now() as update_at
    from types
),

filtred as
(
  select * from signed
  where cur_abbreviation in ('USD', 'EUR', 'RUB')
  )

select * from filtred

{% if is_incremental() %}
  where date::date >= CURRENT_DATE - interval '10 days'
{% endif %}