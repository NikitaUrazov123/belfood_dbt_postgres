with 
source as (
select * from {{ source('Stage1CUpp', 'Д_ВозвратТоваровОтПокупателя') }}
),

renamed_and_cast as 
(
    select 
    --"АдресКлиента",
	--"ВалютаДокумента",
	--"ВидОперации",
	--"ВидПоступления",
	--"ВремяПрибытияПо",
	--"ВремяПрибытияС",
	--"ДатаВходящегоДокумента",
	--"ДиалогСозданиеПеремещения",
	--"ДисконтнаяКарта",
	--"ДлительностьПогрузки",
	--"ДоговорКонтрагента",
	--"ЗаказНаВозврат",
	--"Комментарий",
	--"КонтактноеЛицо",
	--"Контрагент",
	--"КратностьВзаиморасчетов",
	--"КурсВзаиморасчетов",
	--"Менеджер",
	--"Метки",
	--"НесовместимыеМетки",
	--"НомерВходящегоДокумента",
	--"Организация",
	--"Ответственный",
	--"ОтражатьВБухгалтерскомУчете",
	--"ОтражатьВНалоговомУчете",
	--"ОтражатьВУправленческомУчете",
	--"ОтразитьВКнигеПокупок",
	--"Подразделение",
	--"Проект",
	--"Сделка" as "Сделка док. возврата",
	--"СкладОрдер",
	--"СуммаВключаетНДС",
	--"СуммаДокумента",
	--"СчетУчетаНДС",
	--"СчетУчетаРасчетовПоАвансам",
	--"СчетУчетаРасчетовПоТаре",
	--"СчетУчетаРасчетовСКонтрагентом",
	--"ТелефонКонтактногоЛица",
	--"ТипЦен",
	--"ТорговыйОбъект",
	--"УсловиеПродаж",
	--"УчитыватьНДС",
	--"юи_СерияБСО",
	--"юи_ТипБСО",
	--"Проведен",
	"Ссылка" as "Документ возврата",
	--"ПометкаУдаления",
	--"Дата",
	--"Номер",
	--"ПараметрДата",
	--"ВалютаДокументаГуид",
	--"ДисконтнаяКартаГуид",
	--"ДоговорКонтрагентаГуид",
	--"ЗаказНаВозвратГуид",
	--"КонтрагентГуид",
	--"МенеджерГуид",
	--"ОрганизацияГуид",
	--"ОтветственныйГуид",
	--"ПодразделениеГуид",
	--"ПроектГуид",

	"СделкаГуид"::text,

	--"СкладОрдерГуид",
	--"СчетУчетаНДСГуид",
	--"ТипЦенГуид",

	"ТорговыйОбъектГуид"::text,

	--"УсловиеПродажГуид",
	--"юи_ТипБСОГуид",

	"СсылкаГуид"::text

	--"__Partition"
    from source
)

select * from renamed_and_cast