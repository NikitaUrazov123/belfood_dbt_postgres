{{ config(
    materialized='table',
    tags=["exp"]
) }}


with 
base as
(
SELECT 
{{star_exclude_guid(ref('fct_sales'), additional_excludes=["key_record", "Номер строки продаж"])}}
,fct_sales.key_record as key
,{{star_exclude_guid(ref('dim_nomenclature'))}}
,{{star_exclude_guid(ref('dim_calendar'))}}
,{{star_exclude_guid(ref('dim_nbrb_exrates'))}}
,{{star_exclude_guid(ref('dim_client'))}}
,{{star_exclude_guid(ref('dim_sale_docs'), additional_excludes=["key_record", "Номер строки товара реализации"])}}
,{{star_exclude_guid(ref('dim_returns'), additional_excludes=["key_record", "Номер строки документа возврата"])}}
,{{star_exclude_guid(ref('dim_orders'))}}
,dim_country_codes."alpha_3" as "Код страны ISO контрагента"

,coalesce(dim_sale_docs."Гуид торгового объекта", dim_returns."Гуид торгового объекта") as "Гуид торгового объекта"


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
left join {{ ref("dim_country_codes") }}
            on dim_country_codes."Регион" = dim_client."Регион контрагента"         
),


enrichment_shops as
(
    select 
    base.*
    ,{{star_exclude_guid(ref('subdim_shops'))}}
    from base
    left join {{ ref("subdim_shops") }}
                on subdim_shops."СсылкаГуид" = "Гуид торгового объекта"
)


select * from enrichment_shops