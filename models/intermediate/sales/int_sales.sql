with
base as
(
    select
    {{star_exclude_guid(ref("fct_sales"), additional_excludes=["record_id", "Номер строки продаж"])}}
    ,fct_sales.record_id
    ,{{star_exclude_guid(ref("dim_nomenclature"))}}
    ,{{star_exclude_guid(ref('dim_calendar'), additional_excludes=["cdate"])}}
    ,{{star_exclude_guid(ref("dim_clients"))}}
    ,{{star_exclude_guid(ref("dim_sales"), additional_excludes=["record_id"])}}
    ,{{star_exclude_guid(ref("dim_returns"), additional_excludes=["record_id"])}}
    ,{{star_exclude_guid(ref('dim_nbrb_exrates'))}}
    ,dim_country_codes."alpha_3"

    ,coalesce(dim_sales."ТорговыйОбъектГуид",dim_returns."ТорговыйОбъектГуид") as "ТорговыйОбъектГуид"
    ,coalesce(dim_sales."СерияНоменклатурыГуид",dim_returns."СерияНоменклатурыГуид") as "СерияНоменклатурыГуид"
    ,coalesce(dim_sales."ЗаказПокупателяГуид",dim_returns."ЗаказПокупателяГуид") as "ЗаказПокупателяГуид"
    ,coalesce(dim_sales."СкладГуид",dim_returns."СкладГуид") as "СкладГуид"

    from {{ ref("fct_sales") }}
    left join {{ ref("dim_nomenclature") }}
        on fct_sales."НоменклатураГуид"=dim_nomenclature."СсылкаГуид"
    left join {{ ref("dim_calendar") }}
        on fct_sales."Период продаж"=dim_calendar.cdate
    left join {{ ref("dim_clients") }}
        on fct_sales."КонтрагентГуид"=dim_clients."СсылкаГуид"
    left join {{ ref("dim_returns") }}
        on fct_sales.record_id = dim_returns.record_id
    left join {{ ref("dim_sales") }}
        on fct_sales.record_id = dim_sales.record_id
    left join {{ ref("dim_nbrb_exrates") }}
        on fct_sales."Период продаж" = dim_nbrb_exrates."Date"
    left join {{ ref("dim_country_codes") }}
        on dim_country_codes."Регион" = dim_clients."Регион контрагента"   
),

enrichment_shops as
(
    select
    base.*
    ,{{star_exclude_guid(ref("dim_shops"))}}
    from base
    left join {{ ref("dim_shops") }}
        on base."ТорговыйОбъектГуид" = dim_shops."СсылкаГуид"
),

enrichment_nom_series as
(
    select
    enrichment_shops.*
    ,{{star_exclude_guid(ref("dim_nom_series"))}}
    from enrichment_shops
    left join {{ ref("dim_nom_series") }}
        on enrichment_shops."СерияНоменклатурыГуид" = dim_nom_series."СсылкаГуид"
),

enrichment_order_docs as
(
    select
    enrichment_nom_series.*
    ,{{star_exclude_guid(ref("dim_orders"))}}
    from enrichment_nom_series
    left join {{ ref("dim_orders") }}
        on enrichment_nom_series."ЗаказПокупателяГуид" = dim_orders."СсылкаГуид"
),

enrichment_storages as
(
    select
    enrichment_order_docs.*
    ,{{star_exclude_guid(ref("dim_storages"))}}
    from enrichment_order_docs
    left join {{ ref("dim_storages") }}
        on enrichment_order_docs."СкладГуид"= dim_storages."СсылкаГуид"
)

select * from enrichment_storages