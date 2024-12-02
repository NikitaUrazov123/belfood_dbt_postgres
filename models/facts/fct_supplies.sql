{{
    config(
        materialized='view'
    )
}}

with 
base as 
(
select
    "Склад",
	"Номенклатура",
	"Качество",
	"ХарактеристикаНоменклатуры",
	"СерияНоменклатуры",
	key_record,
	"СкладГуид",
	"НоменклатураГуид",
	"КачествоГуид",
	"СерияНоменклатурыГуид",
	"ДатаОстатка",
	updated_at,
	"КоличествоОстаток"
    from 
    {{ ref("stg_РН_ТоварыНаСкладахОстатки") }}
),

renamed as(
    select
    "Склад" as "Склад остатков",
	"Номенклатура" as "Номенклатура остатков",
	"Качество" as "Качество остатков",
	--"ХарактеристикаНоменклатуры",
	"СерияНоменклатуры" as "Серия номенклатуры остатков",
	key_record,
	"СкладГуид",
	"НоменклатураГуид",
	"КачествоГуид",
	"СерияНоменклатурыГуид",
	"ДатаОстатка" as "Дата остатков",
	updated_at,
	"КоличествоОстаток" as "Количество остатков"
    from base
)

select * from renamed