with
base as 
(
    select 
    {{star_exclude_guid(ref("stg_1SUPP__С_Контрагенты"))}}
    ,{{star_exclude_guid(ref("stg_1SUPP__client_props"))}}
    ,{{ ref("stg_1SUPP__С_Контрагенты")}}."СсылкаГуид" as "СсылкаГуид"

    from {{ ref("stg_1SUPP__С_Контрагенты") }} 

    left join {{ ref("stg_1SUPP__client_props") }}
        on "stg_1SUPP__С_Контрагенты"."СсылкаГуид"="stg_1SUPP__client_props"."Гуид контрагента"
)

select * from base