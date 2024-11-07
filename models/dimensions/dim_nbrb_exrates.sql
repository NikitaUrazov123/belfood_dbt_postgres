{{ config(
    materialized='view',
    tags=["subdim"]    
) }}

with source as(
SELECT
    *
FROM
    {{ ref("stg_nbrb_exrates") }}
    ),

EUR_sub as 
(
    select date
    ,Cur_Official_Rate as EUR
    ,Cur_Scale as EUR_scale
    from source
    where Cur_Abbreviation = 'EUR'
),
USD_sub  as 
(
    select date
    ,Cur_Official_Rate as USD
    ,Cur_Scale as USD_scale
    from source
    where Cur_Abbreviation = 'USD'
),
RUB_sub as 
(
    select date
    ,Cur_Official_Rate as RUB
    ,Cur_Scale as RUB_scale
    from source
    where Cur_Abbreviation = 'RUB'
),

joined as
(
    select 
    EUR_sub.date
    ,EUR
    ,EUR_scale
    ,USD
    ,USD_scale
    ,RUB
    ,RUB_scale
    from
    EUR_sub
    left join USD_sub on EUR_sub.date = USD_sub.date
    left join RUB_sub on EUR_sub.date = RUB_sub.date
)

select * from joined

