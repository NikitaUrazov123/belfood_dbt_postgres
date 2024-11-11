{{ config(
    tags=["tst"]
) }}

select key
,COUNT(*) as cnt
from {{ ref("exp_sales") }}
group by key
HAVING COUNT(*) > 1