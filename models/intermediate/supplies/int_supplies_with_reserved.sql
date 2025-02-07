with
united as
(
{{ dbt_utils.union_relations(
    relations=[ref("fct_supplies"), ref("fct_supplies_reserved")],
    exclude=[
        "Номенклатура остатков",
        "Номенклатура рез. остатков",
        "Характеристика номенклатуры",
        "record_id",
        "Серия номенклатуры остатков",
        "Документ рез. остатков",
        "updated_at"]
)}}
),

unifed as 
(
    select
    "Качество остатков"
    ,"СкладГуид"
    ,"НоменклатураГуид"
    ,"КачествоГуид"
    ,"СерияНоменклатурыГуид"
    ,"ДокументРезерваГуид"
    ,case 
        when "Дата рез. остатков" is null
        then 0
        else 1
    end as "Резерв?"
    ,coalesce("Дата остатков", "Дата рез. остатков") as "Дата остатков"
    ,coalesce("Количество остатков", "Количество рез. остатков") as "Количество, шт"
    ,coalesce("Склад остатков", "Склад рез. остатков") as "Склад остатков"
    from united
),

defined_props as 
(
    select
    *
    ,{{ dbt_utils.generate_surrogate_key([
        '\"СкладГуид\"',
        '\"НоменклатураГуид\"',
        '\"КачествоГуид\"',
        '\"СерияНоменклатурыГуид\"',
        '\"ДокументРезерваГуид\"',
        '\"Резерв?\"',
        '\"Дата остатков\"'
    ])}} as record_id
    from unifed
)

select * from defined_props