with
base as 
(
    select * from {{ ref("stg_1SUPP__storages") }}
)

select * from base