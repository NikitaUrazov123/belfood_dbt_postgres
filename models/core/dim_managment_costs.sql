with
base as 
(
    select 
    *
    from {{ ref("stg_1SUPP__managment_costs") }} 
)

select * from base