{{ config(
    materialized='view'
) }}

with 
source as (
select * from {{ source('Stage1CUpp', 'С_ЕдиницыИзмерения') }}),

filtred AS 
(
    SELECT * FROM source 
    where "ПометкаУдаления"=FALSE 
)

select * from filtred