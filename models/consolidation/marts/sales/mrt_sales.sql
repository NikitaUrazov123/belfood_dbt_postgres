{{ config(
    materialized='incremental',
    tags=["incremental"],
    unique_key='key',
    incremental_strategy='delete+insert'
) }}

with 
defined_props_first as
(
    select
    {{star_exclude_guid(ref("exp_sales"), 
    additional_excludes=[
    "Количество продаж" 
    ,"Стоимость продаж" 
    ,"Номер строки продаж" 
    ,"Номер строки док. возврата"
    ,"Номер строки тов. реализации"
    ,"eur"
    ,"eur_scale"
    ,"usd"
    ,"usd_scale"
    ,"rub"
    ,"rub_scale"
    ,"Общее наименование номенклат."
    ])}}
    ,"Количество продаж" as "Количество, шт"
    ,"Стоимость продаж" as "Сумма продажи c НДС, BYN"
    ,"Стоимость продаж"::numeric-"НДС продаж"::numeric as "Сумма продажи без НДС, BYN"
    ,round((("Стоимость продаж"::numeric-"НДС продаж"::numeric)/eur*eur_scale)::numeric,2) as "Сумма продажи без НДС, EUR"
    ,round((("Стоимость продаж"::numeric-"НДС продаж"::numeric)/rub*rub_scale)::numeric,2) as "Сумма продажи без НДС, RUB"
    ,round((("Стоимость продаж"::numeric-"НДС продаж"::numeric)/usd*usd_scale)::numeric,2) as "Сумма продажи без НДС, USD"
    ,coalesce(
        "Серия номен тов. реализации"
        ,"Серия номен док. возврата"
    ) as "Серия номенклатуры"
    ,coalesce(
        "Склад тов. реализации"
        ,"Склад док. реализации"
        ,"Склад док. возврата"
    ) as "Склад"
    ,case 
        when "Код страны ISO контрагента" is not null then "Код страны ISO контрагента" 
        else 'BLR'
    end as "Код страны контрагента"
    ,case 
        when "Общее наименование номенклат." <> '' then  "Общее наименование номенклат."
        else "Наименование номенклат."
    end as "Общее наименование номенклатуры"
    ,case 
        when "Ответственный заказа" <> '' then "Ответственный заказа"
        when "Ответственный заказа" = '' and "Ответственный торгового объекта" <>'' then "Ответственный торгового объекта"
        when "Ответственный заказа" = '' then "Ответственный торгового объекта"
        else null
    end as "Ответственный продажи"
    ,case
        when "Направление продукта номенклат." = 'Давальческое' then 'Давальческое'
        when coalesce("Код страны ISO контрагента", 'BLR')  = 'BLR' then 'Беларусь'
        when "Регион контрагента" like '%comme%' then 'E-commerce'
        when "Регион торгового объекта" like '%comme%' then 'E-commerce'
        else 'Экспорт'
    end as "Категория направления"
    ,case
        when "Количество продаж"<0 then 'Возврат'
        else 'Реализация'
    end as "Тип операции"
    ,date_trunc('MONTH', "Дата производства серии ном.")::date as "Месяц производства номенклатуры"
    ,case 
        when ("Срок годности серии ном."-"Дата производства серии ном.")::numeric = 0 then 0
        else round(("Срок годности серии ном."::date - "Период продаж"::date)::numeric/("Срок годности серии ном."-"Дата производства серии ном.")::numeric,4) 
    end as "Срок реализации, %"
    ,round("Количество продаж"*"Коэф. перевода в литры",2) as "Объем, л"
    from
    {{ ref('exp_sales') }}
),

defined_props_second as
(
    select 
    *
    ,case
        when "Срок реализации, %" <0.2 then '<20%'
        when "Срок реализации, %" between 0.2 and 0.5 then '<50%'
        when "Срок реализации, %" between 0.5 and 0.7 then '<70%'
        when "Срок реализации, %" between 0.7 and 0.8 then '70%-80%'
        when "Срок реализации, %" >= 0.8 then '>80%'
        else '-'
    end as "Реализовано в срок"
    ,case
        when "Объем, л" = 0 then 0
        else "Сумма продажи без НДС, BYN"/"Объем, л"
    end as "Сумма за литр, BYN"
    ,case
        when "Объем, л" = 0 then 0
        else "Сумма продажи без НДС, USD"/"Объем, л" 
    end as "Сумма за литр, USD"
    ,case
        when "Количество, шт" = 0 then 0
        else "Сумма продажи без НДС, BYN"/"Количество, шт" 
    end as "Сумма за штуку, BYN"
    ,case 
        when "Количество, шт" = 0 then 0
        else "Сумма продажи без НДС, USD"/"Количество, шт" 
    end as "Сумма за штуку, USD"
    ,min("Период продаж") over (partition by "Общее наименование номенклатуры") as "Дата первой продажи по артикулу"
    ,case
        when "Наименование номенклат." like '%огурт%' or "Наименование номенклат." like '%удинг%' or "Наименование номенклат." like '%ворож%' then 'Молочное'
        when "Наименование номенклат." like '%каш%' then 'Каши'
        when "Наименование номенклат." like '%смесь%' then 'ЗГМ'
        when "Вид ДП номенклат." like '%пюре%' then 'Пюре'
        else 'Все остальное' 
    end as "Вид ДП доп."
    from defined_props_first
),


filtred as
(
    SELECT * 
    FROM defined_props_second
    WHERE 
        COALESCE("Вид номенклат.", '') IN ('Товар', 'Продукция')
        AND COALESCE("Регион контрагента", '') NOT IN ('Сотрудники', 'Матпомощь', 'Логистика')
        AND COALESCE("Регион торгового объекта", '') NOT IN ('Матпомощь', 'Сотрудники')
        AND COALESCE("Номенклатурная группа", '') NOT IN ('Сырьё-товар', 'Сырьё и материалы', 'Упаковка', 'Упаковка-товар')
        AND COALESCE("Склад", '') NOT IN ('Витебск ГП (опытные партии)')
        AND COALESCE("Наименование торгового объекта", '') != 'ВПК Новка'
        AND (COALESCE("Благ. помощь? док. реализации", 'FALSE') = 'FALSE')
)

SELECT * 
FROM filtred

{% if is_incremental() %}
where "Период продаж"::date >= CURRENT_DATE - interval '2 months'
{% endif %}