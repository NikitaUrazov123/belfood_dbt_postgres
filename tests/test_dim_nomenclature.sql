{{ config(
    tags=["tst"]
) }}

select "СсылкаГуид"
,COUNT(*) as cnt
from {{ ref("dim_nomenclature") }}
group by "СсылкаГуид"
HAVING COUNT(*) > 1