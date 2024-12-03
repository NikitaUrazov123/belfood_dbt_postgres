{{ config(
    materialized='table'
) }}

with
source as
(
    select * from {{ source('Stage1CUpp', 'РН_ПланыПроизводства') }}
)

select * from source
