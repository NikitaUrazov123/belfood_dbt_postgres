{{ config(
    tags=["tst"]
) }}

select
key_record
,count(*)
from {{ ref("stg_Д_РеализТовУслугТовары") }}
group by key_record
HAVING count(*)>1