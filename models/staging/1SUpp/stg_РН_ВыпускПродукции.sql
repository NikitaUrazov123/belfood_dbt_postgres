{{ config(
    materialized='table'
) }}

with
source as
(
    select * from {{ source('Stage1CUpp', 'РН_ВыпускПродукции') }}
),

signed as 
(
    SELECT 
    *
    ,concat("РегистраторГуид", "НомерСтроки") as key_record
    ,now() as updated_at
    FROM source
)

select * from signed