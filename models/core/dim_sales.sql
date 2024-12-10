with
base as 
(
    select 
    {{star_exclude_guid(ref("stg_1SUPP__sale_goods"), additional_excludes=["Номер строки тов. реализации"])}}
    ,{{star_exclude_guid(ref("stg_1SUPP__sale_docs"))}}
    ,{{ ref('stg_1SUPP__sale_docs') }}."ТорговыйОбъектГуид" as "ТорговыйОбъектГуид"
    ,{{ ref("stg_1SUPP__sale_goods") }}."СерияНоменклатурыГуид" as "СерияНоменклатурыГуид"
    ,{{ ref("stg_1SUPP__sale_goods") }}."ЗаказПокупателяГуид" as "ЗаказПокупателяГуид"
    ,{{ ref("stg_1SUPP__sale_docs") }}."СкладГуид" as "СкладГуид"

    from {{ ref("stg_1SUPP__sale_goods") }} 
    left join {{ ref("stg_1SUPP__sale_docs") }} using ("СсылкаГуид")
)

select * from base