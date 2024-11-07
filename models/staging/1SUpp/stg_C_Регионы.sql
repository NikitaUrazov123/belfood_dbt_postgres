{{ config(
    materialized='view'
) }}

with source as (
select * from {{ source('Stage1CUpp', 'С_Регионы') }}),

filtred as
(select * from source
where "ПометкаУдаления" = false)

select * from filtred