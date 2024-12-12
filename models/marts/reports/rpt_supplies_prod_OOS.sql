{{ config(
    materialized='table'
) }}

with 
supplies as
(
    select
    dim_calendar.year,
    dim_calendar.month,
    dim_calendar.yyyymi,
    "Гуид номенклатуры",
    "Дата остатков",
    SUM("Для продажи Витебск") as "Для продажи Витебск, шт",
	--SUM("В резерве") as "Резерв Витебск, шт",
	SUM("Для продажи Заславль") as "Для продажи Заславль, шт",
	--SUM("Резерв Заславль") as "Резерв Заславль, шт",
	SUM("ТЛЦ Орша (ответхранение)") as "ТЛЦ Орша (ответхранение), шт",
	SUM("Витебск (пр-во) карантин") as "Выдержка, шт",
	SUM("Неэтикеровано") as "Неэтикеровано, шт",
	--SUM("СТОП") as "Стоп Витебск, шт",
	--SUM("РЦ Заславль (брак)") as "Заславль брак, шт",
	sum("Маркетплейс") as "Маркетплейс, шт"
	--sum("Резерв Маркетплейс") as "Резерв Маркетплейс, шт"
    from 
    {{ ref('mrt_prod_supplies_2') }}
    left join {{ ref("dim_calendar") }}
            on dim_calendar.cdate::date = "Дата остатков"
    group by 1,2,3,4,5
),

defined_props as
(
    select 
    *,
    "Для продажи Витебск, шт"
    +"Маркетплейс, шт"
    +"Неэтикеровано, шт"
    +"Выдержка, шт"
    +"ТЛЦ Орша (ответхранение), шт"
    +"Для продажи Заславль, шт" as "Общий остаток, шт"
     from supplies
),

oos as
(
    select 
    *
    ,case
        when "Общий остаток, шт"<300 and "Общий остаток, шт">=0 then 1
        else 0
    end as "OOS"
    from defined_props
),

agregated as
(
    select 
    year,
    month,
    yyyymi,
    "Дата остатков",
    "Гуид номенклатуры"
    ,sum("OOS") as "OOS"
    from oos
    group by 1,2,3,4,5
),

defined_props_second as
(
    select
    agregated.*
    ,{{star_exclude_guid(ref('dim_nomenclature'))}}
    from agregated
    left join {{ ref("dim_nomenclature") }}
            on dim_nomenclature."СсылкаГуид" = agregated."Гуид номенклатуры"
    
)

SELECT * 
FROM defined_props_second
