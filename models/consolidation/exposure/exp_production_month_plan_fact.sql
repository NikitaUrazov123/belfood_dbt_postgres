{{ config(
    materialized='view',
) }}


with 
base as
(
SELECT 
{{star_exclude_guid(ref('fct_agr_production_month_plan'), additional_excludes=["key_month_prod"])}}
,{{star_exclude_guid(ref('fct_agr_production_month_fact'), additional_excludes=["key_month_prod"])}}
,coalesce(fct_agr_production_month_plan."НоменклатураГуид",fct_agr_production_month_fact."ПродукцияГуид") as "НоменклатураГуид"
FROM 
{{ ref('fct_agr_production_month_plan') }}
full join {{ ref('fct_agr_production_month_fact') }} using("key_month_prod")
),

defined_props as
(
    select
    (coalesce("Месяц мес. плана производ.","Месяц выпуска прод."))::date as "Месяц производства"
	,coalesce("Подразделение мес. плана производ.","Подразделение выпуска прод.") as "Подразделение производства"
	,coalesce("Номенклатура мес. плана производ.", "Продукция выпуска прод.") as "Номенклатура производства"
	,coalesce("Количество мес. плана производ.",0) as "План производства, шт"
    ,coalesce("Количество выпуска прод.",0) as "Факт производства, шт"
    ,{{star_exclude_guid(ref('dim_nomenclature'))}}
    from base
    left join {{ ref('dim_nomenclature') }}
            on dim_nomenclature."СсылкаГуид" = base."НоменклатураГуид"
),

enrichment_calendar as
(
    select 
    * 
    from defined_props
    left join {{ ref("dim_calendar") }}
        on dim_calendar.cdate = defined_props."Месяц производства"
)

select * from enrichment_calendar