{{ config(
    materialized='table',
    tags=["stg"]
) }}

with
source as (
select * from {{ source('Stage1CUpp', 'С_СерииНоменклатуры') }}),

filtred as
(
    select * from source
    where `ПометкаУдаления` = False
)

select * from filtred
