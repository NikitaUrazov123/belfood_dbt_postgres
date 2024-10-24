{{ config(
    materialized='view',
    tags=["subdim"]
) }}

with source as(
SELECT
    *
FROM
    {{ source('analytics_shipments', 'nbrb_exrates') }}),

filtred as 
(
    select * from source
    where "Cur_Abbreviation" = 'EUR'
),

renamed as
(
    select 
    toDate(`Date`) as date,
    Cur_Scale as scale,
    Cur_OfficialRate as ex_rate
    from filtred
)

select * from renamed

