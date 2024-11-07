{{ config(
    materialized='table',
    tags=["stg"]
) }}

with 
source as 
(SELECT * FROM {{ source('Stage1CUpp', 'С_Номенклатура') }}),

filtred as 
(select * from source
	where 
		"ПометкаУдаления" = False
	and 
		"ЭтоГруппа" = False
)

select * from filtred