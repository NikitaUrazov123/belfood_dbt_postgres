{{ config(
    materialized='table'
) }}

with 
today_supplies as
(
    select
    "Статус номенклат.",
    "Вид ДП номенклат.",
    "Бренд номенклат.",
    "Вид производства номенклат.",
    "Штрихкод",
    "Артикул",
    "Наименование номенклат.",
    "Литраж номенклат.",
    "Гуид номенклатуры",
    "Дата остатков",
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
    where "Дата остатков"::date = CURRENT_DATE::date
    group by 1,2,3,4,5,6,7,8,9,10
),

current_month_production_remains as
(
    select
    "НоменклатураГуид",
    sum("Остаток производства, шт") as "Остаток производства, шт"
    from {{ ref("mrt_production_plan_fact") }}
    where "Месяц производства"::date = date_trunc('month',CURRENT_DATE)
    group by 1
),

joined as
(
    select *
    from today_supplies
    left join current_month_production_remains
        on today_supplies."Гуид номенклатуры"::text = current_month_production_remains."НоменклатураГуид"::text 
)

SELECT * 
FROM joined
