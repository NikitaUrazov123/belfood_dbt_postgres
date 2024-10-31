{{ config(
    materialized='table'
) }}

with source as (
select * from {{ source('Stage1CUpp', 'С_ТорговыеОбъекты') }}),

filtred as
(select * from source
where `ПометкаУдаления` = False)

select * from filtred