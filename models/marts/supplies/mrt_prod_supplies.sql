{{ config(
    materialized='table'
) }}

with
base as
(
    select * from {{ ref("int_supplies_with_reserved_dims") }}
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
    select 
    *
,CASE
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'СТОП(лаборатория)' AND "Наименование склада" = 'Витебск ГП' THEN 'СТОП'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Витебск (пр-во) блокировка ГП' THEN 'СТОП'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Витебск (пр-во) ГП' THEN ' '
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Витебск (пр-во) ГП1' THEN 'Неэтикеровано'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Витебск (пр-во) ГП2' THEN 'Неэтикеровано'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Витебск (пр-во) ГП3' THEN 'Неэтикеровано'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Склад ТЛЦ Орша-Белтаможсервис (ответхранение)' THEN 'ТЛЦ Орша (ответхранение)'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Выдержка' AND "Наименование склада" = 'Витебск ГП' THEN 'Выдержка'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Выдержка' AND "Наименование склада" = 'Витебск (пр-во) ГП' THEN 'Выдержка'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Выдержка' AND "Наименование склада" = 'Витебск (пр-во) ГП1' THEN 'Выдержка'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Выдержка' AND "Наименование склада" = 'Витебск (пр-во) ГП2' THEN 'Выдержка'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Выдержка' AND "Наименование склада" = 'Витебск (пр-во) ГП3' THEN 'Выдержка'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Склад АгроСтальСтрой (ответхранение) брак' THEN 'РЦ Заславль (брак)'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Склад АгроСтальСтрой (ответхранение)' THEN 'РЦ Заславль'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Витебск ГП' THEN 'Для продажи Витебск'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Маркетплейс' THEN 'Маркетплейс'
    WHEN "Резерв?" = 1 AND "Наименование склада" = 'Витебск ГП' THEN 'Резерв'
    WHEN "Резерв?" = 1 AND "Наименование склада" = 'Маркетплейс' THEN 'Резерв маркетплейс'
    WHEN "Резерв?" = 1 AND "Наименование склада" = 'Склад АгроСтальСтрой (ответхранение)' THEN 'Резерв Заславль'
    ELSE 'не определенно'
END AS "Комментарий",

CASE
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'СТОП(лаборатория)' AND "Наименование склада" = 'Витебск ГП' THEN 'Витебск ГП'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'СТОП(лаборатория)' AND "Наименование склада" = 'Склад АгроСтальСтрой (ответхранение)' THEN 'Витебск ГП'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Витебск (пр-во) блокировка ГП' THEN 'Витебск ГП'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Витебск (пр-во) ГП' THEN 'Неэтикеровано'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Витебск (пр-во) ГП1' THEN 'Неэтикеровано'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Витебск (пр-во) ГП2' THEN 'Неэтикеровано'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Витебск (пр-во) ГП3' THEN 'Неэтикеровано'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Склад ТЛЦ Орша-Белтаможсервис (ответхранение)' THEN 'ТЛЦ Орша (ответхранение)'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Выдержка' AND "Наименование склада" = 'Витебск ГП' THEN 'Витебск ГП'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Выдержка' AND "Наименование склада" = 'Витебск (пр-во) ГП' THEN 'Витебск ГП'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Выдержка' AND "Наименование склада" = 'Витебск (пр-во) ГП1' THEN 'Витебск ГП'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Выдержка' AND "Наименование склада" = 'Витебск (пр-во) ГП2' THEN 'Витебск ГП'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Выдержка' AND "Наименование склада" = 'Витебск (пр-во) ГП3' THEN 'Витебск ГП'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Склад АгроСтальСтрой (ответхранение) брак' THEN 'РЦ Заславль'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Склад АгроСтальСтрой (ответхранение)' THEN 'РЦ Заславль'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Витебск ГП' THEN 'Витебск ГП'
    WHEN "Резерв?" = 0 AND "Наименование качества" = 'Новый' AND "Наименование склада" = 'Маркетплейс' THEN 'Маркетплейс'
    WHEN "Резерв?" = 1 THEN 'Витебск ГП'
    ELSE 'не определенно'
END AS "Склад"
from filtred
---------------------------------------------------------------------------------------------
),

defined_props_second as
(
    select 
    *
    ,CASE
        WHEN "Комментарий" = 'Выдержка' THEN 1
        ELSE 0
    END AS "Выдержка?"
    from defined_props_first
),

defined_props_third as
(
    select 
    *
    ,CASE 
    WHEN "Комментарий" = 'Резерв' THEN "Количество, шт"
    ELSE 0
END AS "В резерве"
from defined_props_second
),

defined_props_fourth as
(
    select
    *,
CASE
    WHEN (("Склад" = 'Витебск ГП' AND "Выдержка?" = 0) OR ("Склад" = 'РЦ Заславль' AND "Выдержка?" = 0)) THEN 
        CASE 
            WHEN "Комментарий" = 'Резерв' THEN "В резерве" * -1
            WHEN "Комментарий" <> 'Для продажи Витебск' THEN 0
            ELSE "Количество, шт"
        END
    ELSE 0
END AS "Для продажи Витебск",

CASE 
    WHEN "Комментарий" = 'СТОП' THEN "Количество, шт"
    ELSE 0
END AS "СТОП",

CASE 
    WHEN "Комментарий" = 'Для продажи Витебск' THEN "Количество, шт"
    ELSE 0
END AS "Витебск ГП",

CASE
    WHEN "Комментарий" = 'Резерв Заславль' THEN "Количество, шт" * -1
    WHEN "Комментарий" = 'РЦ Заславль' THEN "Количество, шт"
    ELSE 0
END AS "Для продажи Заславль",

CASE
    WHEN "Комментарий" = 'Резерв Болбасово' THEN "Количество, шт" * -1
    WHEN "Комментарий" = 'ТЛЦ Орша (ответхранение)' THEN "Количество, шт"
    ELSE 0
END AS "ТЛЦ Орша (ответхранение)",

CASE 
    WHEN "Комментарий" = 'Резерв Заславль' THEN "Количество, шт"
    ELSE 0
END AS "Резерв Заславль",

CASE 
    WHEN "Комментарий" = 'РЦ Заславль (брак)' THEN "Количество, шт"
    ELSE 0
END AS "РЦ Заславль (брак)",

CASE 
    WHEN "Комментарий" = 'РЦ Заславль (стоп)' THEN "Количество, шт"
    ELSE 0
END AS "РЦ Заславль (стоп)",

CASE
    WHEN "Комментарий" = 'Резерв маркетплейс' THEN "Количество, шт" * -1
    WHEN "Комментарий" = 'Маркетплейс' THEN "Количество, шт"
    ELSE 0
END AS "Маркетплейс",

CASE 
    WHEN "Комментарий" = 'Резерв маркетплейс' THEN "Количество, шт"
    ELSE 0
END AS "Резерв Маркетплейс",

CASE 
    WHEN "Склад" = 'Неэтикеровано' THEN "Количество, шт"
    ELSE 0
END AS "Неэтикеровано",

CASE 
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
from defined_props_third
),

defined_props_fifth as
(
    select 
    *
    ,CASE 
        WHEN "Срок годности, дн" = 0 
            THEN 0 
        ELSE round(("Срок реализации, дн"::numeric / "Срок годности, дн"), 4)::real
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
    END AS "Сроки"
    from defined_props_fourth
)

select * from defined_props_fifth