{{ config(
    materialized='view'
) }}

with source as(
SELECT * FROM {{ ref("stg_api_nbrb_by_exrates__exrates") }})

select * from source