{{ config(
    tags=["tst"]
) }}

select "СсылкаГуид"
,COUNT(*) as cnt
from {{ ref("stg_С_Номенклатура") }}
group by "СсылкаГуид"
HAVING COUNT(*) > 1