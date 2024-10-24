{{ config(
    materialized='view',
    tags=["dim"]
) }}

select * from {{ ref("stg_Д_ЗаказПокупателя") }}