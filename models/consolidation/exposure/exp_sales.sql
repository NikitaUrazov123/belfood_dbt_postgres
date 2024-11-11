{{ config(
    materialized='view',
    tags=["exp"]
) }}

SELECT 
{{star_exclude_guid(ref('fct_sales'), additional_excludes=["key_record"])}},
{{star_exclude_guid(ref('dim_nomenclature'))}},
{{star_exclude_guid(ref('dim_calendar'))}},
{{star_exclude_guid(ref('dim_nbrb_exrates'))}},
{{star_exclude_guid(ref('dim_client'))}},
{{star_exclude_guid(ref('dim_sale_docs'), additional_excludes=["key_record"])}},
{{star_exclude_guid(ref('dim_returns'), additional_excludes=["key_record"])}},
{{star_exclude_guid(ref('dim_orders'))}}

FROM 
{{ ref('fct_sales') }}
left join {{ ref('dim_nomenclature') }}
            on fct_sales."НоменклатураГуид"= dim_nomenclature."СсылкаГуид"
left join {{ ref("dim_calendar") }}
            on fct_sales."Период продаж"::date = dim_calendar.cdate::date
left join {{ ref("dim_nbrb_exrates") }}
            on dim_nbrb_exrates.date::date = fct_sales."Период продаж"::date
left join {{ ref("dim_client") }}
            on dim_client."СсылкаГуид" = fct_sales."КонтрагентГуид"
left join {{ ref("dim_sale_docs") }}
            on fct_sales.key_record = dim_sale_docs.key_record
left join {{ ref("dim_returns") }}
            on fct_sales.key_record = dim_returns.key_record
left join {{ ref("dim_orders") }}
            on dim_orders."СсылкаГуид" = fct_sales."ЗаказПокупателяГуид"     
