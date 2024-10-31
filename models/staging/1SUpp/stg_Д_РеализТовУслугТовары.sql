{{ config(
    materialized='incremental'
) }}

with
source as 
(
    select * from {{ source('Stage1CUpp', 'Д_РеализацияТоваровУслугТовары') }})
    
select * from source