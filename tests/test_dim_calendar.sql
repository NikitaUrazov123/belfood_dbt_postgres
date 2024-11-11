{{ config(
    tags=["tst"]
) }}

select cdate
,COUNT(*) as cnt
from {{ ref("dim_calendar") }}
group by cdate
HAVING COUNT(*) > 1