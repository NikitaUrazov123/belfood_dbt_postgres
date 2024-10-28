{{ config(
    materialized='view',
    tags=["dim"]
) }}

select * from {{ source('Stage1CUpp', 'Д_РеализацияТоваровУслугТовары') }}