with
base as 
(
    select * from {{ ref("stg_1SUPP__managment_costs") }} 
),

matrix as --определяем матрицу из всех месяцев и всей номенклатуры
(
    select distinct -- декартово произведение всех месяцев и всей номенклатуры, представленной в исходном файле
    b1."Месяц упр. себес."::date,
    b2."НоменклатураГуид"
    from base b1
    cross join base b2

    union all -- union зависает

    select distinct -- декартово произведение для возможных остутсвующих месяцев, начиная от последнего в файле до текущего
    date_trunc(
        'month', generate_series(
            (select max("Месяц упр. себес.") from base )::timestamp,
            current_date,
            '1 day'))::date as "Месяц упр. себес."
    ,b2."НоменклатураГуид"
    from base b1 
    cross join base b2
),

deduplicate_matrix as
(
    select distinct * from matrix 
),

base_prices as
(
    select 
    deduplicate_matrix."Месяц упр. себес.",
    deduplicate_matrix."НоменклатураГуид",
    base."Себестроимость упр. себес."
    from deduplicate_matrix
    left join base 
        on base."Месяц упр. себес."=deduplicate_matrix."Месяц упр. себес."
        and base."НоменклатураГуид"=deduplicate_matrix."НоменклатураГуид"
),

present_check AS (
    SELECT 
        "НоменклатураГуид"
        ,"Месяц упр. себес."
        ,"Себестроимость упр. себес."
        ,case
            when "Себестроимость упр. себес." is null then 0 else 1
        end as present_check -- если себестоимость заполнена в исходнике, то 1 иначе 0 
        from base_prices
),

row_groups as 
(
    select 
    *
    ,sum(present_check) over (partition by "НоменклатураГуид" order by "Месяц упр. себес.") as row_group
    from present_check 
),

filled_costs as -- если значение для номенклатуры отсутсвует для месяца, то заполняется предыдущей известной
(
    select * 
    ,case 
        when "Себестроимость упр. себес." is not null then "Себестроимость упр. себес."
        else first_value("Себестроимость упр. себес.") over (partition by "НоменклатураГуид", row_group order by "Месяц упр. себес.")
    end as filled_costs
    from row_groups
),

defined_costs as
(
    SELECT 
    "НоменклатураГуид"
    ,"Месяц упр. себес."
    ,filled_costs AS "Себестроимость упр. себес."
FROM 
    filled_costs
)

select * from defined_costs
--where "НоменклатураГуид"='dd15b047-bf1b-11ed-817d-000c29d648be'
--ORDER BY 1,2 desc