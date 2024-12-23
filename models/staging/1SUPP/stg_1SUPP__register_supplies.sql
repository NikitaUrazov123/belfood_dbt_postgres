with
base as 
(
    select * from {{ ref("base_1SUPP__register_supplies") }}
),

renamed_and_cast as 
(
    select 
    CAST("Склад" AS VARCHAR(80)) as "Склад остатков", 
    CAST("Номенклатура" AS VARCHAR(150)) as "Номенклатура остатков", 
    CAST("Качество" AS VARCHAR(50)) as "Качество остатков",
    CAST("СерияНоменклатуры" AS VARCHAR(70)) as "Серия номенклатуры остатков",
    "СкладГуид"::text, 
    "НоменклатураГуид"::text, 
    "КачествоГуид"::text, 
    "СерияНоменклатурыГуид"::text,
    "ДатаОстатка"::date as "Дата остатков",
    CAST("КоличествоОстаток" as real) as "Количество остатков",
    record_id
    from base
)

select * from renamed_and_cast
