select date, COUNT(*) as cnt
from {{ ref("dim_calendar") }}
group by date
having cnt>1

