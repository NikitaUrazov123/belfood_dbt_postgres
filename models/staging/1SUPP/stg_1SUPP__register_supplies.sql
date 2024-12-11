with
base as 
(
    select * from {{ ref("base_1SUPP__register_supplies") }}
),

filtred as
(
    select *
    from base
    where "ДатаОстатка"::date >= CURRENT_DATE - INTERVAL '6 months'
),

renamed as 
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
    CAST("КоличествоОстаток" as real) as "Количество остатков"
    from filtred
),

defined_props as 
(
    SELECT
    *
    ,{{ dbt_utils.generate_surrogate_key([
        '\"Дата остатков\"',
        '\"СерияНоменклатурыГуид\"',
        '\"КачествоГуид\"',
        '\"НоменклатураГуид\"',
        '\"СкладГуид\"'
    ])}} as record_id
    from renamed
)

select * from defined_props
