{{ config(
    materialized='view',
    tags=["subdim"]
) }}

with
source as
(
    select * from {{ ref("stg_С_ЕдиницыИзмерения") }}
),

filtred as
(
    select * from source 
    where source."Наименование" = 'л'
),

renamed AS
(
select
"Объем"/"Коэффициент" as "Коэфициент в литры",
--"Владелец",
--"Наименование",
"ВладелецГуид"
from filtred
)

select * from renamed