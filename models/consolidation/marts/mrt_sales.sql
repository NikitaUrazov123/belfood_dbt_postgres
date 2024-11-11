{{ config(
    materialized='table'
) }}

with 
defined_props as
(
    select
    {{star_exclude_guid(ref("exp_sales"), 
        additional_excludes=[
            "Номер строки продаж", 
            "Номер строки документа возврата",
            "Номер строки товара реализации",
            "key",
            "eur",
            "eur_scale",
            "usd",
            "usd_scale",
            "rub",
            "rub_scale"])}}
    ,"Стоимость продаж"*eur/eur_scale as "Сумма продажи, EUR"
    ,"Стоимость продаж"*rub/rub_scale as "Сумма продажи, RUB"
    ,"Стоимость продаж"*usd/usd_scale as "Сумма продажи, USD"
    ,coalesce(
        "Серия номен товара реализации",
        "Серия номен документа возврата") as "Серия номенклатуры"
            
    from
    {{ ref('exp_sales') }}
)


select * from defined_props