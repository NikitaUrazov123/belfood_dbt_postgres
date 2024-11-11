{{ config(
    materialized='view'
) }}

with
source as 
(
    select * from {{ source('Stage1CUpp', 'РН_ТоварыВРезНаСкладОстат') }}
),

signed as 
(
    SELECT
    *
    FROM source
) 

select * from signed

