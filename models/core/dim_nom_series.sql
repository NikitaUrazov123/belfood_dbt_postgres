with
base as 
(
    select 
    {{star_exclude_guid(ref("stg_1SUPP__nom_series"))}}
    ,{{ ref("stg_1SUPP__nom_series")}}."СсылкаГуид" as "СсылкаГуид"
    from {{ ref("stg_1SUPP__nom_series") }} 
)

select * from base