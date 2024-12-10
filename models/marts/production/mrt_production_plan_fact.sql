with 
base as
(
SELECT 
{{star_exclude_guid(ref('fct_agr_production_month_plan'), additional_excludes=["month_prod_key"])}}
,{{star_exclude_guid(ref('fct_agr_production_month_fact'), additional_excludes=["month_prod_key"])}}
,coalesce(fct_agr_production_month_plan.month_prod_key, fct_agr_production_month_fact.month_prod_key) as month_prod_key
,coalesce(fct_agr_production_month_plan."НоменклатураГуид",fct_agr_production_month_fact."ПродукцияГуид") as "НоменклатураГуид"
FROM 
{{ ref('fct_agr_production_month_plan') }}
full join {{ ref('fct_agr_production_month_fact') }} using("month_prod_key")
),

defined_props as
(
    select
    month_prod_key
    ,coalesce("Месяц плана производ.","Месяц выпуска прод.") as "Месяц производства"
	,coalesce("Подразделение плана производ.","Подразделение выпуска прод.") as "Подразделение производства"
	,coalesce("Количеств плана производ.",0) as "План производства, шт"
    ,coalesce("Количество выпуска прод.",0) as "Факт производства, шт"
    ,{{star_exclude_guid(ref('dim_nomenclature'))}}
    ,dim_nomenclature."СсылкаГуид" as "НоменклатураГуид"
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
),

filtred as
(
    select * from enrichment_calendar
    where "Вид номенклат." != 'Материалы'
),

defined_props_2 as 
(
    select 
    *
    ,case
        when "План производства, шт" - "Факт производства, шт" > 0 then "План производства, шт" - "Факт производства, шт"
        else 0
    end as "Остаток производства, шт"
    from filtred
)

select * from defined_props_2