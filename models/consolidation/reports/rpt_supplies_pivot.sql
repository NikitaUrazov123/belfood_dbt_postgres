{{ config(
    materialized='view'
) }}

with 
today_supplies as
(
    select
    "Статус номенклат.",
    "Вид ДП номенклат.",
    "Бренд номенклат.",
    "Направление продукта номенклат.",
    "Штрихкод номенклат.",
    "Артикул",
    "Наименование номенклат.",
    "Литраж номенклат.",
    "НоменклатураГуид" as "ОстНоменклатураГуид",
    "Дата",
    SUM("Для продажи Витебск") as "Для продажи Витебск, шт",
	SUM("В резерве") as "Резерв Витебск, шт",
	SUM("Для продажи Заславль") as "Для продажи Заславль, шт",
	SUM("Резерв Заславль") as "Резерв Заславль, шт",
	SUM("ТЛЦ Орша (ответхранение)") as "ТЛЦ Орша (ответхранение), шт",
	SUM("Витебск (пр-во) карантин") as "Выдержка, шт",
	SUM("Неэтикеровано") as "Неэтикеровано, шт",
	SUM("СТОП") as "Стоп Витебск, шт",
	SUM("РЦ Заславль (брак)") as "Заславль брак, шт",
	sum("Маркетплейс") as "Маркетплейс, шт",
	sum("Резерв Маркетплейс") as "Резерв Маркетплейс, шт"
    from 
    {{ ref('mrt_prod_supplies') }}
    where "Дата"::date = CURRENT_DATE::date
    group by 1,2,3,4,5,6,7,8,9,10
),

current_month_production_remains as
(
    select
    "НоменклатураГуид" as "ПроизвНоменклатураГуид",
    sum("Остаток производства, шт") as "Остаток производства, шт"
    from {{ ref("mrt_production_month_plan_fact") }}
    where "Месяц производства"::date = date_trunc('month',CURRENT_DATE)
    group by 1
),

package_supplies as 
(
    select 
    "ГуидНоменклатураГП",sum("Количество, шт") as "Остаток упаковки, шт"
    from {{ ref("exp_supplies_with_reserved") }}  
    where "Резерв?"=0
    and "ГуидНоменклатураГП" is not null
    and "Дата"::date = CURRENT_DATE::date
    and "Склад" in (
'Витебск (пр-во) сырье Цех 1', 
'Витебск (пр-во) сырье Цех 2', 
'Витебск (пр-во) сырье Цех 3', 
'Витебск (пр-во) сырье Цех 4',
'Витебск (пр-во) ГП4',
'Витебск (давальческое сырье и материалы)',
'Витебск (сырье и материалы) блокировка',
'Витебск (сырье и материалы)')
and "Вид номенклат." = 'Упаковка'
group by 1
),

joined as
(
    select *
    from today_supplies
    left join current_month_production_remains
        on today_supplies."ОстНоменклатураГуид"::text = current_month_production_remains."ПроизвНоменклатураГуид"::text 
    left join package_supplies
        on today_supplies."ОстНоменклатураГуид"::text  = package_supplies."ГуидНоменклатураГП"::text 
)

SELECT * 
FROM joined
