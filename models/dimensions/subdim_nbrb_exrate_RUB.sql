{{ config(
    materialized='view',
    tags=["subdim"]
) }}

with source as(
SELECT
    *
FROM
    {{ ref("stg_nbrb_exrates") }}),

filtred as 
(
    select * from source
    where "Cur_Abbreviation" = 'RUB'
),

renamed as
(
    select 
    date,
    Cur_Scale as scale,
    Cur_OfficialRate as ex_rate
    from filtred
)

select * from renamed

