{{ config(
    materialized='incremental'
) }}

with
source as 
(
    select * from {{ source('Stage1CUpp', 'РН_ТоварыНаСкладахОстатки') }}
),

signed as 
(
    SELECT
    *
    ,now() as updated_at 
    FROM source
) 

select * from signed

{% if is_incremental() %}
  where "ДатаОстатка" >= dateAdd(day, -10, today())
{% endif %}
