with
base as 
(
    select 
    {{star_exclude_guid(ref("stg_1SUPP__managment_costs"))}}
    from {{ ref("stg_1SUPP__managment_costs") }} 
)

select * from base