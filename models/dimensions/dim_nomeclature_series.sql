{{ config(
    tags=["dim"]
) }}

with 
source as
(
    select * from {{ ref("stg_С_СерииНоменклатуры") }}
),

renamed as
(
    select 
    --"СерийныйНомер" as "Серийный номер серии номенклатуры",
	"СрокГодности" as "Срок годности серии номенклатуры",
	--"НомерГТД",
	"СтранаПроисхождения" as "Страна происхождения серии номенклатуры",
	--"ОсновноеИзображение",
	"Комментарий" as "Комментарий серии номенклатуры",
	"ДатаПроизводства" as "Дата производства серии номенклатуры",
	--"НомерУдостоверенияКачества",
	--"КоличествоЕдиницПотребТары",
	--"МассоваяДоляСухихВеществ",
	--"ОбъемнаяДоляСпирта",
	--"Кислотность",
	--"Цвет",
	--"МассоваяДоляДвуокисиУглерода",
	--"ДатаРозлива",
	--"НеМенятьКоличествоСерии",
	"СтопПродаж" as "Стоп продаж? серии номенклатуры",
	"Выдержка" as "Выдержка? серии номенклатуры",
	--"Сертификация",
	--"КороткиеСроки",
	"ДатаСтопа" as "Дата стопа серии номенклатуры",
	"ДатаОкончанияСтопа" as "Дата окончания стопа серии номенклатуры",
	"Производитель" as "Производитель серии номенклатры",
	"СтатусПартии" as "Статус партии серии номенклатуры",
	"СрокГодностиВДнях" as "Срок годности в днях серии номенклатуры",
	"Сертификат" as "Сертификат серии номенклатуры",
	"ФлагСобственнаяУпаковка" as "Собственная упаковка? серии номенклатуры",
	--"юи_Дизайн",
	--"Предопределенный",
	--"Ссылка",
	--"ПометкаУдаления",
	"Владелец" as "Номенклатура серии номенклатуры",
	"Наименование" as "Наименование серии номенклатуры",
	--"Код",
	--"ПараметрНаименование",
	--"НомерГТДГуид",
	--"СтранаПроисхожденияГуид",
	--"ПроизводительГуид",
	--"СертификатГуид",
	--"юи_ДизайнГуид",
	"СсылкаГуид",
	"ВладелецГуид"
	--"__Partition"
    from source
)

select * from renamed 