with
base as 
(
    select 
    {{star_exclude_guid(ref('stg_1SUPP__С_Номенклатура'))}}
    ,{{star_exclude_guid(ref('stg_1SUPP__nomenclature_props'))}}
    ,{{star_exclude_guid(ref('stg_1SUPP__barcodes'))}}
    ,{{ ref('stg_1SUPP__С_Номенклатура')}}."СсылкаГуид" as "СсылкаГуид"

    from {{ ref("stg_1SUPP__С_Номенклатура") }} 

    left join {{ ref("stg_1SUPP__nomenclature_props") }}
        on "stg_1SUPP__С_Номенклатура"."СсылкаГуид"="stg_1SUPP__nomenclature_props"."Гуид номенклатуры"

    left join {{ ref("stg_1SUPP__barcodes") }}
        on "stg_1SUPP__С_Номенклатура"."СсылкаГуид" = "stg_1SUPP__barcodes"."ВладелецГуид"
)

select * from base