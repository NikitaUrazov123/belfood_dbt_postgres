with 
agregated_sales as 
(
    select
    "Период продаж"
    ,"НоменклатураГуид"
    ,{{ dbt_utils.generate_surrogate_key(['\"Период продаж\"', '\"НоменклатураГуид\"'])}} as key
    ,sum("Количество, шт") as "Количество продано, шт"
    from {{ ref("rpt_sales_import_prods") }}
    where "Период продаж"::date >= CURRENT_DATE - INTERVAL '6 months'
    group by 1,2
),

agregated_supplies as 
(
    select
    "Дата остатков"
    ,"Гуид номенклатуры"
    ,{{ dbt_utils.generate_surrogate_key(['\"Дата остатков\"', '\"Гуид номенклатуры\"'])}} as key
    ,sum("Количество, шт") as "Количество остатка, шт"
    from {{ ref("rpt_prod_supplies_import_prods") }}
    where "Резерв?" = 0
    group by 1,2
),

joined as
(
    select
    coalesce("Период продаж", "Дата остатков") as "Период"
    ,coalesce("НоменклатураГуид", "Гуид номенклатуры") as "НоменклатураГуид"
    ,"Количество продано, шт"
    ,"Количество остатка, шт"
    from agregated_sales
    full join agregated_supplies
    using (key)
),

enrichment as 
(
    select
    *
    from
    joined
    left join {{ ref("dim_nomenclature") }}
        on joined."НоменклатураГуид" = dim_nomenclature."СсылкаГуид"
    left join {{ ref("dim_calendar") }}
        on joined."Период" = dim_calendar.cdate

)

select * from enrichment
