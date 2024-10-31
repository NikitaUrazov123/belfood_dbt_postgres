--Надо добавить scale
{{ config(
    tags=["dim"]
) }}

with 
USD as
(select * from {{ ref("subdim_nbrb_exrate_USD") }}),

RUB as
(select * from {{ ref("subdim_nbrb_exrate_RUB") }}),

EUR as
(select * from {{ ref("subdim_nbrb_exrate_EUR") }}),

joined as 
(
select * from 
USD 
join RUB on USD.date = RUB.date
join EUR on USD.date = EUR.date 
),

renamed as 
(
    select 
    USD.date as date,
    USD.ex_rate as USD,
    RUB.ex_rate as RUB,
    EUR.ex_rate as EUR
    from
    joined
)

select * from renamed
