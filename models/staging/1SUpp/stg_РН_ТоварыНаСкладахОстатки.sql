{{ config(
    materialized='incremental',
    unique_key='key_record',
    tags=["incremental"]
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
    ,concat(to_char("ДатаОстатка", 'DD-MM-YYYY HH:MI:SS'),
            "СерияНоменклатурыГуид",
            "ХарактеристикаНоменклатуры",
            "КачествоГуид",
            "НоменклатураГуид",
            "СкладГуид") as key_record
    ,now() as updated_at 
    FROM source
),

columns_order_data_types as
(
    select 
    CAST("Склад" AS VARCHAR(80)) as "Склад", 
    CAST("Номенклатура" AS VARCHAR(150)) as "Номенклатура", 
    CAST("Качество" AS VARCHAR(50)) as "Качество", 
    CAST("ХарактеристикаНоменклатуры" AS VARCHAR(50)) as "ХарактеристикаНоменклатуры", 
    CAST("СерияНоменклатуры" AS VARCHAR(70)) as "СерияНоменклатуры",
    CAST(key_record AS VARCHAR(180)) as key_record,
    "СкладГуид", 
    "НоменклатураГуид", 
    "КачествоГуид", 
    "СерияНоменклатурыГуид",
    "ДатаОстатка",
    CURRENT_TIMESTAMP as updated_at,
    CAST("КоличествоОстаток" as real) as "КоличествоОстаток"
    from signed
    order by "ДатаОстатка" desc
)

select * from columns_order_data_types


{% if is_incremental() %}
  where "ДатаОстатка"::date >= CURRENT_DATE - interval '3 days'
{% endif %}
