with
base as
(
    select * from {{ ref("int_supplies_with_reserved") }}
)

select * from base