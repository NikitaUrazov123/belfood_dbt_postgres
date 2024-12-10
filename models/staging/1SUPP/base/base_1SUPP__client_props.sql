{{ config(
    materialized='view'
) }}

with
source as 
(
    SELECT * FROM {{ source('Stage1CUpp', 'РС_ЗначенияСвойствОбъектов') }}
),

client_record_ids as 
(
    SELECT 
    "СсылкаГуид"::text as "Гуид контрагента" 
    from {{ source('Stage1CUpp', 'С_Контрагенты') }}
),

filtred as
(
    select
    *
    from source
    left join client_record_ids
        on source."ОбъектГуид"::text = client_record_ids."Гуид контрагента"::text
    where "Гуид контрагента" is not null
    and "Свойство" in ('Тип канала', 'Торговое наименование (если сеть)')
),

final as
(
    select 
    "Гуид контрагента",
    "Свойство",
    CASE
        when "Значение" = '' then null
        else "Значение" 
    end as "Значение" 
    from filtred
)

select * from final