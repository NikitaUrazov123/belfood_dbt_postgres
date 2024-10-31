{{ config(
    materialized='view'
) }}

with
source as 
(
    select * from {{ source('Stage1CUpp', 'Д_РеализацияТоваровУслуг') }}),

filtred as 
(
    select * from source
    where `ПометкаУдаления` = False
)
select * from filtred