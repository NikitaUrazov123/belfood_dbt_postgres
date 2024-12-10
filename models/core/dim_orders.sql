with
base as 
(
    select 
    {{star_exclude_guid(ref("stg_1SUPP__order_docs"))}}
    ,{{ ref("stg_1SUPP__order_docs")}}."СсылкаГуид" as "СсылкаГуид"
    from {{ ref("stg_1SUPP__order_docs") }} 
)

select * from base