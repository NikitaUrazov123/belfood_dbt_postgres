{{ config(
    materialized='view',
    tags=["exp"]
) }}

with
base as
(
    select * from {{ ref("exp_supplies_with_reserved") }}
),

filtred as
(
    select * from base
    where "Вид номенклат." = 'Продукция'
    or "Вид номенклат." = 'Товар'
),

defined_props_first as
(
    select 
    *
    ,CASE
        WHEN "Резерв?" = 0 AND "Качество" = 'СТОП(лаборатория)' AND "Склад" = 'Витебск ГП' THEN 'СТОП'
        WHEN "Резерв?" = 0 AND "Качество" = 'Новый' AND "Склад" = 'Витебск (пр-во) блокировка ГП' THEN 'СТОП'
        WHEN "Резерв?" = 0 AND "Качество" = 'Новый' AND "Склад" IN ('Витебск (пр-во) ГП', 'Витебск (пр-во) ГП1', 'Витебск (пр-во) ГП2', 'Витебск (пр-во) ГП3') THEN 'Неэтикеровано'
        WHEN "Резерв?" = 0 AND "Качество" = 'Новый' AND "Склад" = 'Склад ТЛЦ Орша-Белтаможсервис (ответхранение)' THEN 'ТЛЦ Орша (ответхранение)'
        WHEN "Резерв?" = 0 AND "Качество" = 'Выдержка' THEN 'Выдержка'
        WHEN "Резерв?" = 0 AND "Качество" = 'Новый' AND "Склад" LIKE 'Склад АгроСтальСтрой%' THEN CASE
            WHEN "Склад" LIKE '%брак' THEN 'РЦ Заславль (брак)'
            ELSE 'РЦ Заславль'
        END
        WHEN "Резерв?" = 0 AND "Качество" = 'Новый' THEN CASE
            WHEN "Склад" = 'Витебск ГП' THEN 'Для продажи Витебск'
            WHEN "Склад" = 'Маркетплейс' THEN 'Маркетплейс'
        END
        WHEN "Резерв?" = 1 THEN CASE
            WHEN "Склад" = 'Витебск ГП' THEN 'Резерв'
            WHEN "Склад" = 'Маркетплейс' THEN 'Резерв маркетплейс'
            WHEN "Склад" = 'Склад АгроСтальСтрой (ответхранение)' THEN 'Резерв Заславль'
        END
        ELSE 'не определенно'
    END AS "Комментарий"
    from filtred
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
)

select * from defined_props_second