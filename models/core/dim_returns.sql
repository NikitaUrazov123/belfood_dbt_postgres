with
base as 
(
    select 
    {{star_exclude_guid(ref("stg_1SUPP__return_goods"), additional_excludes=["Номер строки док. возврата"])}}
    ,{{star_exclude_guid(ref("stg_1SUPP__return_docs"))}}   
    ,{{ ref('stg_1SUPP__return_docs') }}."ТорговыйОбъектГуид" as "ТорговыйОбъектГуид"
    ,{{ ref("stg_1SUPP__return_goods") }}."СерияНоменклатурыГуид" as "СерияНоменклатурыГуид"
    ,{{ ref("stg_1SUPP__return_goods") }}."ЗаказПокупателяГуид" as "ЗаказПокупателяГуид"
    ,{{ ref("stg_1SUPP__return_goods") }}."СкладГуид" as "СкладГуид"

    from {{ ref("stg_1SUPP__return_goods") }} 
    left join {{ ref("stg_1SUPP__return_docs") }} USING ("СсылкаГуид")
)

select * from base