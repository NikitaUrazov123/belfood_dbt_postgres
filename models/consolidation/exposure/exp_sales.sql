{{ config(
    materialized='view',
    tags=["exp"]
) }}

SELECT 
{{star_exclude_guid(ref('fct_sales'), additional_excludes=["key_record"])}},
{{star_exclude_guid(ref('dim_nomenclature'))}},
{{star_exclude_guid(ref('dim_calendar'))}},
{{star_exclude_guid(ref('dim_nbrb_exrates'))}},
{{star_exclude_guid(ref('dim_sale_docs_goods'), additional_excludes=["key_record"])}},
{{star_exclude_guid(ref('dim_sale_docs'))}},
{{star_exclude_guid(ref('dim_orders'))}}
FROM 
{{ ref('fct_sales') }} as fct
left join {{ ref('dim_nomenclature') }} as dim_nom 
            on fct."НоменклатураГуид"= dim_nom."СсылкаГуид"
left join {{ ref("dim_calendar") }} as dim_cal
            on fct."Период продаж"::date = dim_cal.cdate::date
left join {{ ref("dim_nbrb_exrates") }} as dim_exrates
            on dim_exrates.date::date = fct."Период продаж"::date
left join {{ ref("dim_sale_docs_goods") }} as dim_sale_goods
            on dim_sale_goods.key_record = fct.key_record
left join {{ ref("dim_sale_docs") }} as dim_sd
            on dim_sd."СсылкаГуид" = dim_sale_goods."СсылкаГуид" 
left join {{ ref("dim_orders") }} as dim_orders
            on dim_orders."СсылкаГуид" = dim_sd."СделкаГуид"