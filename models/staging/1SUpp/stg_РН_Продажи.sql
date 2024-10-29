{{ config(
    materialized='incremental',
    tags=["stg"]
) }}

with
source as
(
    select * from {{ source('Stage1CUpp', 'РН_Продажи') }}
),

signed as 
(
    SELECT 
    *,
    now() as updated_at
    FROM source
)

select * from signed

{% if is_incremental() %}
  where "Период" >= dateAdd(month, -1, today())
{% endif %}
