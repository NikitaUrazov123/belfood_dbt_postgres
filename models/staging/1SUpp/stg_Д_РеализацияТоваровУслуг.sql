{{ config(
    materialized='view'
) }}

select * from {{ source('Stage1CUpp', 'Д_РеализацияТоваровУслуг') }}