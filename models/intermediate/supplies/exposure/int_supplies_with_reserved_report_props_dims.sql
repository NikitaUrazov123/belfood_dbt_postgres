with
base as
(
    select
    {{star_exclude_guid(ref("int_supplies_with_reserved_add_report_props"))}}
    ,{{star_exclude_guid(ref("dim_storages"))}}
    ,{{star_exclude_guid(ref("dim_nomenclature"))}}
    ,dim_nomenclature."СсылкаГуид" as "Гуид номенклатуры"
    ,{{star_exclude_guid(ref("dim_quality"))}}
    ,{{star_exclude_guid(ref("dim_nom_series"))}}
    ,{{star_exclude_guid(ref("stg_1SUPP__order_docs"))}}
    from {{ ref("int_supplies_with_reserved_add_report_props") }}
    left join {{ ref("dim_storages") }}
        on int_supplies_with_reserved_add_report_props."СкладГуид" = dim_storages."СсылкаГуид"
    left join {{ ref("dim_nomenclature") }}
        on int_supplies_with_reserved_add_report_props."НоменклатураГуид" = dim_nomenclature."СсылкаГуид"
    left join {{ ref("dim_quality") }}
        on int_supplies_with_reserved_add_report_props."КачествоГуид" = dim_quality."СсылкаГуид"
    left join {{ ref("dim_nom_series") }}
        on int_supplies_with_reserved_add_report_props."СерияНоменклатурыГуид" = dim_nom_series."СсылкаГуид"
    left join {{ ref("stg_1SUPP__order_docs") }} as orders
        on int_supplies_with_reserved_add_report_props."ДокументРезерваГуид" = orders."СсылкаГуид"
)

select * from base