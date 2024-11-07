{{ config(
    materialized='incremental',
    unique_key='key_record'
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
    ,concat(to_char("ДатаОстатка", 'DD-MM-YYYY HH:MI:SS'), "СерияНоменклатурыГуид","ХарактеристикаНоменклатуры", "КачествоГуид", "НоменклатураГуид", "СкладГуид") as key_record
    ,now() as updated_at 
    FROM source
) 

select * from signed

{% if is_incremental() %}
  where "ДатаОстатка"::date >= CURRENT_DATE - interval '10 days'
{% endif %}
