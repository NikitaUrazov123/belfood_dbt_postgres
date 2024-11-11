{{ config(
    materialized='table'
) }}

with 
base as
(
    select
    {{star_exclude_guid(ref("exp_sales"))}}
    from
    {{ ref('exp_sales') }}
)

select * from base