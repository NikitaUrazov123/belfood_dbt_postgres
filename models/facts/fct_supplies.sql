{{
    config(
        materialized='view',
        tags =["fct"]
    )
}}

select
    *
from {{ ref("stg_РН_ТоварыНаСкладахОстатки") }}