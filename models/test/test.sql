{{ config(
    materialized='view'
) }}


SELECT
3 as customer_id,
'nikita'as name,
'new' as status