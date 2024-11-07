{{
    config(
        materialized='view'
    )
}}

select
    *
from {{ ref("stg_РН_ТоварыНаСкладахОстатки") }}