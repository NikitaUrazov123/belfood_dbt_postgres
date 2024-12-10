with
base as 
(
    select 
    {{star_exclude_guid(ref("stg_1SUPP__shops"))}}
    ,{{ ref("stg_1SUPP__shops")}}."СсылкаГуид" as "СсылкаГуид"
    from {{ ref("stg_1SUPP__shops") }} 
)

select * from base