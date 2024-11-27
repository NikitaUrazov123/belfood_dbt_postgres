{{ config(
    materialized='view'
) }}

with
source as (
select * from {{ source('Stage1CUpp', 'Д_КоррКачестваТоваровТовары') }}
)

select * from source
