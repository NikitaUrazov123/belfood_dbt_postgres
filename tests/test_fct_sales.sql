{{ config(
    tags=["tst"]
) }}

select
key_record
,count(*)
from {{ ref("fct_sales") }}
group by key_record
HAVING count(*)>1