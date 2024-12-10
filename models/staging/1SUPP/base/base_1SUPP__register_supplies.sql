{{
    config(
        materialized='table'
    )
}}

with
source as 
(
    select * from {{ source('Stage1CUpp', 'РН_ТоварыНаСкладахОстатки') }}
),

agregated as 
(
    select 
    "Склад",
    "Номенклатура",
    "Качество",
    "СерияНоменклатуры",
    "СкладГуид", 
    "НоменклатураГуид", 
    "КачествоГуид", 
    "СерияНоменклатурыГуид",
    "ДатаОстатка",
    sum("КоличествоОстаток") as "КоличествоОстаток"
    from source
    where "ДатаОстатка"::date >= CURRENT_DATE
    group by 1,2,3,4,5,6,7,8,9
),

defined_props as 
(
    SELECT
    *
    ,{{ dbt_utils.generate_surrogate_key([
        '\"ДатаОстатка\"',
        '\"СерияНоменклатурыГуид\"',
        '\"КачествоГуид\"',
        '\"НоменклатураГуид\"',
        '\"СкладГуид\"'
    ])}} as record_id
    from agregated
)

select * from defined_props
