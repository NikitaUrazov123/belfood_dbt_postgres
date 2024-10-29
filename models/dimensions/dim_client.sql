{{ config(
    materialized='view',
    tags=["dim"]
) }}

select * from {{ ref("stg_С_Контрагенты") }}