{{ config(
    materialized='view'
) }}

select * from {{ source('Stage1CUpp', 'С_Контрагенты') }}
  