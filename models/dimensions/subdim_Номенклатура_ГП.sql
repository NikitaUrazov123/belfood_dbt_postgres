{{ config(
    materialized='view',
    tags=["subdim"]
) }}


with 
filtred as
(
    select * from {{ ref('stg_РС_ЗначенияСвойствОбъектов') }}
    WHERE
    "Свойство" = 'Номенклатура ГП'
),

renamed as
(
    select
    "ОбъектГуид"
    ,"ЗначениеГуид"
    ,"Значение" as "Св-во. Номенклатура ГП"
    from filtred
)

select * from renamed


