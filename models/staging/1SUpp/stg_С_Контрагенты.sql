{{ config(
    materialized='view',
) }}

with 
source as
(
    select * from {{ source('Stage1CUpp', 'С_Контрагенты') }}
),

filtred as
(
    select * from source
    --where "ПометкаУдаления" = False and
     where "ЭтоГруппа" =False
)

select * from filtred