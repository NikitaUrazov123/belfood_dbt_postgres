with
base as
(
    select * from {{ ref("int_supplies_with_reserved_report_props_dims") }}
),

filtred as
(
    select * from base
    where "Вид номенклат." in('Продукция','Товар')
    and "Наименование склада" in (
        'Склад АгроСтальСтрой (ответхранение) брак',
        'Склад АгроСтальСтрой (ответхранение)',
        'Маркетплейс',
        'Витебск ГП', 
        'Витебск (пр-во) ГП3', 
        'Витебск (пр-во) ГП2', 
        'Витебск (пр-во) ГП', 
        'Витебск (пр-во) блокировка ГП', 
        'Склад ТЛЦ Орша-Белтаможсервис (ответхранение)')
),

defined_props_first as 
(
select *
,CASE 
    WHEN "Выдержка?" = 1 AND "Дата окончания стопа серии ном."::date > CURRENT_DATE THEN "Количество, шт"
    ELSE 0
END AS "Витебск (пр-во) карантин",

CASE 
    WHEN "Срок годности серии ном." is null THEN ''
    ELSE "Срок годности серии ном."::text
END AS "Срок годности серии",

CASE 
    WHEN "Дата производства серии ном." is null THEN ''
    ELSE "Дата производства серии ном."::text
END AS "Дата производства",

CASE 
    WHEN "Дата окончания стопа серии ном." is null THEN ''
    ELSE "Дата окончания стопа серии ном."::text
END AS "Дата окончания выдержки",

("Срок годности серии ном." - "Дата производства серии ном.") AS "Срок годности, дн",

CASE 
    WHEN ("Срок годности серии ном."::date -"Дата остатков"::date) < 0 THEN 0
    ELSE ("Срок годности серии ном."::date -"Дата остатков"::date)
END AS "Срок реализации, дн"
from filtred
),

defined_props_second as
(
    select 
    *
,CASE 
    WHEN "Срок годности, дн" = 0 
        THEN '0.00%'
    ELSE 
        to_char(
            round("Срок реализации, дн"::numeric / "Срок годности, дн", 4)*100, 
            'FM999990.00%'
        )
END AS "Срок реализации, %",


    CASE 
        WHEN "Резерв?" = '1' THEN 'Резерв'
        WHEN "Дата производства" = '' THEN '-'
        WHEN "Резерв?" = '0' AND "Дата производства" <> '' AND "Срок реализации, дн" <= 0 THEN 'Просрочено'
        WHEN round(("Срок реализации, дн"::numeric / "Срок годности, дн") * 100, 2) < 20 THEN '<20%'
        WHEN round(("Срок реализации, дн"::numeric / "Срок годности, дн") * 100, 2) BETWEEN 20 AND 49.99 THEN '<50%'
        WHEN round(("Срок реализации, дн"::numeric / "Срок годности, дн") * 100, 2) BETWEEN 50 AND 69.99 THEN '<70%'
        WHEN round(("Срок реализации, дн"::numeric / "Срок годности, дн") * 100, 2) BETWEEN 70 AND 79.99 THEN '70%-80%'
        WHEN round(("Срок реализации, дн"::numeric / "Срок годности, дн") * 100, 2) >= 80 THEN '>80%'
        ELSE '-' 
    END AS "Сроки",
    case
        WHEN round(("Срок реализации, дн"::numeric / "Срок годности, дн") * 100, 2) > 66 THEN 'Да'
        ELSE 'Нет'
    END AS "Срок более 66%?"
    from defined_props_first
)

select * from defined_props_second