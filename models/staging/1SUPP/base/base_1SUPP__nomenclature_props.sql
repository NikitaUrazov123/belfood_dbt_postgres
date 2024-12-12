{{ config(
    materialized='view'
) }}

with
source as 
(
    SELECT * FROM {{ source('Stage1CUpp', 'РС_ЗначенияСвойствОбъектов') }}
),

nomenclature_record_ids as 
(
    SELECT 
    "СсылкаГуид"::text as "Гуид номенклатуры" 
    from {{ source('Stage1CUpp', 'С_Номенклатура') }}
),

filtred as
(
    select
    *
    from source
    left join nomenclature_record_ids
        on source."ОбъектГуид"::text = nomenclature_record_ids."Гуид номенклатуры"::text
    where "Гуид номенклатуры" is not null
),

formed as
(
    select 
    "Гуид номенклатуры",
    "Свойство",
    "Значение"
    from filtred
),

taste_to_lower as
(
    select 
    "Гуид номенклатуры"
    ,"Свойство"
    ,CASE
        when "Свойство" = 'мВкус' then lower(replace(replace(trim("Значение" ),' ', '_'), '-', '_'))
        else "Значение" 
    end as "Значение"
    from formed
)

select * from taste_to_lower