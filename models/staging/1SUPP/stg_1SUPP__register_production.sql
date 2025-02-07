with
source as
(
    select * from {{ source('Stage1CUpp', 'РН_ВыпускПродукции') }}
),

renamed_and_cast as
(
    select
    --"КодОперации",
	--"ПодразделениеПолучатель",
	--"ЗаказПолучатель",
	--"СтатьяЗатратПолучатель",
	--"НоменклатурнаяГруппаПолучатель",
	"СкладПолучатель" as "Склад получатель выпуска прод.",
	--"СтатьяЗатратПоВозврату",
	--"ПродукцияПолучатель",
	--"СерияПродукцииПолучатель",
	--"СтатусПартии",
	"ВариантВыпускаПродукции" as "Варинат выпуска прод.",
	--"ОбъектСтроительства",
	--"НомерСтрокиДокумента",
	--"Активность",
	"НомерСтроки" as "Номер строки выпуска прод.",
	"Регистратор" as "Регистратор выпуска прод.",
	"Период" as "Период выпуска прод.",
	"Подразделение" as "Подразделение выпуска прод.",
	--"НоменклатурнаяГруппа",
	"Продукция" as "Продукция выпуска прод.",
	--"ХарактеристикаПродукции",
	"СерияПродукции" as "Серия выпуска прод.",
	"Спецификация" as "Спецификация выпуска прод.",
	--"Заказ",
	"ДокументВыпуска" as "Документ выпуска прод.",
	--"Качество",
	"Количество" as "Количество выпуска прод.",
	--"КоличествоДоделка",
	"Стоимость" as "Стоимость выпуска прод.",
	--"ПараметрПериод",
	--"ПодразделениеПолучательГуид",
	--"ЗаказПолучательГуид",
	--"СтатьяЗатратПолучательГуид",
	--"СкладПолучательГуид",
	--"СтатьяЗатратПоВозвратуГуид",
	--"ПродукцияПолучательГуид",
	--"СерияПродукцииПолучательГуид",
	--"ОбъектСтроительстваГуид",
	"РегистраторГуид"::text,
	--"ПодразделениеГуид",
	--"НоменклатурнаяГруппаГуид",
	"ПродукцияГуид"::text
	--"СерияПродукцииГуид",
	--"СпецификацияГуид",
	--"ЗаказГуид",
	--"ДокументВыпускаГуид",
	--"КачествоГуид",
	--"__Partition"
    from source
),

defined_props as 
(
    SELECT 
    *
    ,{{ dbt_utils.generate_surrogate_key(['\"РегистраторГуид\"', "\"Номер строки выпуска прод.\""]) }} as record_id
    --,now() as updated_at
    FROM renamed_and_cast
)

select * from defined_props