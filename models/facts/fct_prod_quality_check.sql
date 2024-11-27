{{
    config(
        materialized='view'
    )
}}


with
source as 
(
    select * from {{ ref("stg_Д_КоррКачестваТоваровТовары") }}
),

signed as
(
    select 
    *
    ,concat("СсылкаГуид"::text, "НомерСтроки"::text) as key_record
    from source
),

renamed as 
(
    select
    "Ссылка" AS "Товар док. кор. качества",
	"НомерСтроки" as "Номер строки товара док. кор. качества",
	--"ЕдиницаИзмеренияМест",
	"Качество" as "Старое качество товара док. кор. качества",
	"Количество" as "Количество товара док. кор. качества",
	--"КоличествоМест",
	--"Коэффициент",
	"Номенклатура" as "Номенклатура товара док. кор. качества",
	"СерияНоменклатуры" as "Серия номенклатуры товара док. кор. качества",
	--"СчетУчетаБУ",
	--"СчетУчетаНУ",
	--"ХарактеристикаНоменклатуры",
	"КачествоНовое" as "Новое качество товара док. кор. качества.",
	--"ЕдиницаИзмерения",
	"Склад" as "Склад товара док. кор. качества",
	--"Дата" as "Д",
	--"ПараметрДата",
	"СсылкаГуид",
	"ЕдиницаИзмеренияМестГуид",
	"КачествоГуид",
	"НоменклатураГуид",
	"СерияНоменклатурыГуид",
	"СчетУчетаБУГуид",
	"СчетУчетаНУГуид",
	"ХарактеристикаНоменклатурыГуид",
	"КачествоНовоеГуид",
	"ЕдиницаИзмеренияГуид",
	"СкладГуид",
	"__Partition",
    key_record
    from signed
)

select * from renamed