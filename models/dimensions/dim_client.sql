{{ config(
    materialized='view',
    tags=["dim"]
) }}

select * from {{ source('Stage1CUpp', 'С_Контрагенты') }}