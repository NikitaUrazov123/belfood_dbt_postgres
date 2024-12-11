with
source as (
select * from {{ source('Stage1CUpp', 'С_СерииНоменклатуры') }}
),

renamed_and_cast as
(
    select 
    --"СерийныйНомер",
	"СрокГодности" as "Срок годности серии ном.",
	--"НомерГТД",
	--"СтранаПроисхождения",
	--"ОсновноеИзображение",
	--"Комментарий",
	"ДатаПроизводства"::date as "Дата производства серии ном.",
	--"НомерУдостоверенияКачества",
	--"КоличествоЕдиницПотребТары",
	--"МассоваяДоляСухихВеществ",
	--"ОбъемнаяДоляСпирта",
	--"Кислотность",
	--"Цвет",
	--"МассоваяДоляДвуокисиУглерода",
	--"ДатаРозлива",
	--"НеМенятьКоличествоСерии",
	--"СтопПродаж",
	--"Выдержка",
	--"Сертификация",
	--"КороткиеСроки",
	"ДатаСтопа"::date as "Дата стопа серии ном.",
	"ДатаОкончанияСтопа"::date as "Дата окончания стопа серии ном.",
	--"Производитель",
	--"СтатусПартии",
	--"СрокГодностиВДнях",
	--"Сертификат",
	--"ФлагСобственнаяУпаковка",
	--"юи_Дизайн",
	--"Предопределенный",
	--"Ссылка",
	--"ПометкаУдаления",
	"Владелец" as "Номенклатура серии ном.",
	"Наименование" as "Наименование серии ном.",
	--"Код",
	--"ПараметрНаименование",
	--"НомерГТДГуид",
	--"СтранаПроисхожденияГуид",
	--"ПроизводительГуид",
	--"СертификатГуид",
	--"юи_ДизайнГуид",
	"СсылкаГуид"::text,
	"ВладелецГуид"::text
	--"__Partition"
    from source

)

select * from renamed_and_cast
