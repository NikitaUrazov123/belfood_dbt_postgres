with 
source as (
select * from {{ source('Stage1CUpp', 'Д_РеализацияТоваровУслуг') }}
),

renamed_and_cast as 
(
    select 
   	"АдресДоставки" as "Адрес доставки док. реализации",
	--"БанковскийСчетОрганизации",
	--"ВалютаДокумента",
	--"ВидОперации",
	--"ВидПередачи",
	--"Грузоотправитель",
	--"Грузополучатель",
	--"ДисконтнаяКарта",
	--"ДоговорКонтрагента",
	"Комментарий" as "Комментарий док. реализации",
	--"Контрагент",
	--"КратностьВзаиморасчетов",
	--"КурсВзаиморасчетов",
	--"Организация",
	"Ответственный" as "Ответственный док. реализации",
	--"ОтражатьВБухгалтерскомУчете",
	--"ОтражатьВНалоговомУчете",
	--"ОтражатьВУправленческомУчете",
	--"Подразделение",
	--"Проект",
	"Сделка" as "Сделка док. реализации",
	--"Склад" as "Склад док. реализации",
	--"СуммаВключаетНДС",
	--"СуммаДокумента",
	--"СчетУчетаРасчетовПоАвансам",
	--"СчетУчетаРасчетовПоТаре",
	--"СчетУчетаРасчетовСКонтрагентом",
	"ТипЦен" as "Тип цен док. реализации",
	--"УчитыватьНДС",
	--"СчетУчетаДоходовПоТареБУ",
	--"СчетУчетаДоходовПоТареНУ",
	--"СчетУчетаРасходовПоТареБУ",
	--"СчетУчетаРасходовПоТареНУ",
	--"ОтпускРазрешил",
	--"ОтпускПроизвел",
	--"ДоверенностьНомер",
	--"ДоверенностьДата",
	--"ДоверенностьВыдана",
	--"ДоверенностьЧерезКого",
	--"ОтключитьКонтрольВзаиморасчетов",
	--"УсловиеПродаж",
	--"ДополнениеКАдресуДоставки",
	--"ТорговыйОбъект",
	"СерияБСО" as "Серия ТТН док. реализации",
	"НомерБСО" as "Номер ТТН док. реализации",
	--"СерияБСОТара",
	--"НомерБСОТара",
	--"Водитель",
	"ПутевойЛист"  as "Путевой лист док. реализации",
	--"Автомобиль",
	--"Прицеп",
	--"Заказчик",
	--"ВладелецТранспорта",
	--"ОснованиеОтпуска",
	--"ТараОтдельно",
	--"ПереданыДокументы",
	--"Менеджер",
	"юи_БлаготворительнаяПомощь" as "Благ. помощь? док. реализации",
	--"ДатаОтгрузки",
	--"ВремяПрибытияС",
	--"ВремяПрибытияПо",
	--"ДлительностьРазгрузки",
	--"Метки",
	--"НесовместимыеМетки",
	--"КонтактноеЛицо",
	--"ТелефонКонтактногоЛица",
	--"юи_ТипБСО",
	--"ДатаСчетФактуры",
	--"НомерСчетФактуры",
	--"НомерCMR",
	--"ДатаCMR",
	--"Проведен",
	"Ссылка" as "Документ реализации",
	--"ПометкаУдаления",
	--"Дата",
	--"Номер",
	--"ПараметрДата",
	--"БанковскийСчетОрганизацииГуид",
	--"ВалютаДокументаГуид",
	--"ГрузоотправительГуид",
	--"ГрузополучательГуид",
	--"ДисконтнаяКартаГуид",
	--"ДоговорКонтрагентаГуид",
	--"ОтветственныйГуид",
	--"ПодразделениеГуид",
	--"ПроектГуид",
	"СделкаГуид"::text,
	"СкладГуид"::text,
	--"СчетУчетаРасчетовПоАвансамГуид",
	--"СчетУчетаРасчетовПоТареГуид",
	"ТипЦенГуид"::text,
	--"СчетУчетаДоходовПоТареБУГуид",
	--"СчетУчетаДоходовПоТареНУГуид",
	--"СчетУчетаРасходовПоТареБУГуид",
	--"СчетУчетаРасходовПоТареНУГуид",
	--"ОтпускРазрешилГуид",
	--"ОтпускПроизвелГуид",
	--"УсловиеПродажГуид",
	"ТорговыйОбъектГуид"::text,
	--"юи_ТипБСОГуид",
	--"КонтрагентГуид",
	"СсылкаГуид"::text
	--"ЗаказчикГуид",
	--"__Partition"
    from source
)

select * from renamed_and_cast