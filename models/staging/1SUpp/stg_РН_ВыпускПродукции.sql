{{ config(
    materialized='table'
) }}

with
source as
(
    select * from {{ source('Stage1CUpp', 'РН_ВыпускПродукции') }}
)

select * from source