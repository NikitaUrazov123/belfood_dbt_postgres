with 
base as 
(
select 
{{dbt_utils.star(ref("stg_1SUPP__register_reserved_supplies"))}}
,now() as updated_at
from {{ ref("stg_1SUPP__register_reserved_supplies") }}
)

select * from base