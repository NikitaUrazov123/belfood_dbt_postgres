{{ config(
    tags=["tst"]
) }}

select "ВладелецГуид"
,COUNT(*) as cnt
from {{ ref("subdim_Штрихкоды") }}
group by "ВладелецГуид"
HAVING COUNT(*) > 1