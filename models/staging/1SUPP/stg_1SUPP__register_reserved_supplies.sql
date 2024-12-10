with
source as 
(
    select * from {{ source('Stage1CUpp', 'РН_ТоварыВРезНаСкладОстат') }}
),

filtred as
(
    select *
    from source
    where "ДатаОстатка"::date >= CURRENT_DATE
),

renamed as 
(
    select 
    CAST("Склад" AS VARCHAR(80)) as "Склад рез. остатков", 
    CAST("Номенклатура" AS VARCHAR(150)) as "Номенклатура рез. остатков", 
    CAST("ДокументРезерва" AS VARCHAR(70)) as "Документ рез. остатков",
    "СкладГуид"::text, 
    "НоменклатураГуид"::text, 
    "ДокументРезерваГуид"::text, 
    "ДатаОстатка"::date as "Дата рез. остатков",
    CAST("КоличествоОстаток" as real)  as "Количество рез. остатков"
    from filtred
),

defined_props as 
(
    SELECT
    *
    ,{{ dbt_utils.generate_surrogate_key([
        '\"Дата рез. остатков\"', 
        '\"НоменклатураГуид\"',
        '\"СкладГуид\"',
        '\"ДокументРезерваГуид\"',
        ]) }} as record_id
    FROM renamed
)

select * from defined_props