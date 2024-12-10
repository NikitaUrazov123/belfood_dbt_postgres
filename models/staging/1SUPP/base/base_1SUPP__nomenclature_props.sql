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

final as
(
    select 
    "Гуид номенклатуры",
    "Свойство",
    "Значение"
    from filtred
)

select * from final