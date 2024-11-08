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
{{star_exclude_guid(ref('dim_orders'))}},
{{star_exclude_guid(ref('dim_purchase_returns'))}},
{{star_exclude_guid(ref('dim_nomeclature_series'))}},
{{star_exclude_guid(ref('dim_client'))}}
FROM 
{{ ref('fct_sales') }}
left join {{ ref('dim_nomenclature') }}
            on fct_sales."НоменклатураГуид"= dim_nomenclature."СсылкаГуид"
left join {{ ref("dim_calendar") }}
            on fct_sales."Период продаж"::date = dim_calendar.cdate::date
left join {{ ref("dim_nbrb_exrates") }}
            on dim_nbrb_exrates.date::date = fct_sales."Период продаж"::date
left join {{ ref("dim_sale_docs_goods") }}
            on dim_sale_docs_goods.key_record = fct_sales.key_record
left join {{ ref("dim_sale_docs") }}
            on dim_sale_docs."СсылкаГуид" = dim_sale_docs_goods."СсылкаГуид" 
left join {{ ref("dim_orders") }}
            on dim_orders."СсылкаГуид" = dim_sale_docs."СделкаГуид"
left join {{ ref("dim_purchase_returns") }}
            on dim_purchase_returns."СсылкаГуид" = fct_sales."РегистраторГуид"
left join {{ ref("dim_nomeclature_series") }}
            on dim_nomeclature_series."СсылкаГуид" = dim_sale_docs_goods."СерияНоменклатурыГуид"
left join {{ ref("dim_client") }}
            on dim_client."СсылкаГуид" = fct_sales."КонтрагентГуид"
            
