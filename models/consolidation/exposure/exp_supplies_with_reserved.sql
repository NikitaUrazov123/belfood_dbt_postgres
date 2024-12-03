{{ config(
    materialized='table',
    tags=["exp"]
) }}

with 
base as
(
    (
    select
    0 as "Резерв?",
    "Склад остатков" as "Склад",
	"Номенклатура остатков" as "Номенклатура",
    "Качество остатков" as "Качество",
    null as "Документ рез. остатков",
	"Серия номенклатуры остатков" as "Серия номенклатуры остатков",
	key_record,
	"СкладГуид",
	"НоменклатураГуид",
	"СерияНоменклатурыГуид",
    null as "ДокументРезерваГуид",
	"Дата остатков" as "Дата",
	updated_at,
	"Количество остатков" as "Количество, шт"
    FROM
    {{ ref('fct_supplies') }}
    where "Дата остатков"::date >= CURRENT_DATE - interval '2 months'
    )
union all
(
    select
    1 as "Резерв?",
    "Склад рез. остатков" as "Склад",
	"Номенклатура рез. остатков" as "Номенклатура",
    null as "Качество",
	"Документ рез. остатков" as "Документ рез. остатков",
    null as "Серия номенклатуры остатков",
	key_record,
	"СкладГуид",
	"НоменклатураГуид",
    null as "СерияНоменклатурыГуид",
	"ДокументРезерваГуид",
	"Дата рез. остатков" as "Дата",
	updated_at,
	"Количество рез. остатков" as "Количество, шт"
    FROM
    {{ ref('fct_supplies_reserved') }}
    where "Дата рез. остатков"::date >= CURRENT_DATE - interval '2 months'
)
),

joined as
(
    select
    "Резерв?"
    ,"Склад"
    ,"Номенклатура"
    ,"Качество"
    ,"Документ рез. остатков"
    ,"Серия номенклатуры остатков"
    ,key_record
    ,"Дата"
    ,"Количество, шт"
    ,{{star_exclude_guid(ref('subdim_nomeclature_series'))}}
    ,{{star_exclude_guid(ref('dim_nomenclature'))}}
    ,{{star_exclude_guid(ref('dim_orders'))}}
    ,{{star_exclude_guid(ref('dim_calendar'))}}
    from 
    base
    left join {{ ref('subdim_nomeclature_series') }} 
                on base."СерияНоменклатурыГуид" = subdim_nomeclature_series."СсылкаГуид"
    left join {{ ref('dim_nomenclature') }}
                on base."НоменклатураГуид" = dim_nomenclature."СсылкаГуид"
    left join {{ ref('dim_orders') }}
                on base."ДокументРезерваГуид" = dim_orders."СсылкаГуид"
    left join {{ ref('dim_calendar') }}
                on base."Дата"::date = dim_calendar.cdate::date
)

select * from joined