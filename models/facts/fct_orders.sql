{{
    config(
        materialized='view'
    )
}}

SELECT * fROM {{ ref("stg_РН_ЗаказыПокупателей") }}
