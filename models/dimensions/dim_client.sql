{{ config(
    tags=["dim"]
) }}

with
source as
(
    select * from {{ ref("stg_С_Контрагенты") }}
),

chain_prop as
(
	select * from {{ ref("subdim_Торговое_наименование") }}
),

sales_chanel_prop as
(
	select * from {{ ref("subdim_Тип_канала") }}
),

renamed as
(
    select
    "НаименованиеПолное" as "Полное наименование контрагента",
	--"ДокументУдостоверяющийЛичность",
	--"ГоловнойКонтрагент",
	--"ИсточникИнформацииПриОбращении",
	--"КодПоОКПО",
	--"Комментарий",
	--"ИНН",
	--"КПП",
	--"ОсновноеКонтактноеЛицо",
	--"ОсновнойБанковскийСчет",
	--"ОсновнойВидДеятельности",
	--"ОсновнойДоговорКонтрагента",
	--"ОсновнойМенеджерПокупателя",
	--"Покупатель",
	--"Поставщик",
	--"РасписаниеРаботыСтрокой",
	--"СрокВыполненияЗаказаПоставщиком",
	--"ЮрФизЛицо",
	--"НеЯвляетсяРезидентом",
	--"ОКОПФ",
	"Регион" as "Регион контрагента",
	--"ГруппаДоступаКонтрагента",
	--"юи_Код77",
	--"юи_МинимальнаяСуммаЗаказа",
	--"ДополнительноеОписание",
	--"юи_СтранаРегистрации",
	--"НеОбъединятьСтрокиТТН",
	--"юи_Давальцы",
	--"МС_ВыводитьСтрокуВПримечаниеТТН",
	--"МС_СтрокаВПримечанииТТН",
	--"юи_ГруппаДляТТН",
	--"ВзаимозависимоеЛицо",
	--"Предопределенный",
	--"Ссылка",
	--"ПометкаУдаления",
	--"ЭтоГруппа",
	--"Родитель",
	"Наименование" as "Наименование контрагента",
	--"Код",
	--"ПараметрНаименование",
	--"ГоловнойКонтрагентГуид",
	--"ОсновноеКонтактноеЛицоГуид",
	--"ОсновнойБанковскийСчетГуид",
	--"ОсновнойВидДеятельностиГуид",
	--"ОсновнойДоговорКонтрагентаГуид",
	--"ОКОПФГуид",
	--"РегионГуид",
	--"ГруппаДоступаКонтрагентаГуид",
	--"юи_СтранаРегистрацииГуид",
	chain_prop."Значение" as "Торг. наименование контрагента",
	sales_chanel_prop."Значение" as "Тип канала контрагента",
	"СсылкаГуид"
	--"РодительГуид",
	--"__Partition"
	
	from source
	left join chain_prop
	on source."СсылкаГуид"=chain_prop."ОбъектГуид"
	left join sales_chanel_prop
	on source."СсылкаГуид"=sales_chanel_prop."ОбъектГуид"

)

select * from renamed



