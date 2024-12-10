{{ config(
    materialized='view'
) }}

with source as (
select * from {{ source('Stage1CUpp', 'С_Качество') }}),

renamed as
(
    select
	--"Предопределенный",
	--"Ссылка",
	--"ПометкаУдаления",
	"Наименование" as "Наименование качества",
	--"Код",
	--"ПараметрНаименование",
	"СсылкаГуид"::text
	--"__Partition"
    from source
)

select * from renamed