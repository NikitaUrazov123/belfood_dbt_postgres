select
key_record
,count(*)
from {{ ref("stg_РН_Продажи") }}
group by key_record
HAVING count(*)>1