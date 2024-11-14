{{ config(
    tags=["dim"]
) }}

with 
source as 
(
	select * from {{ ref("stg_Д_ВозврТоварОтПокупат") }}
),

renamed as
(
select 
/*"АдресКлиента",
	"ВалютаДокумента",
	"ВидОперации",
	"ВидПоступления",
	"ВремяПрибытияПо",
	"ВремяПрибытияС",
	"ДатаВходящегоДокумента",
	"ДиалогСозданиеПеремещения",
	"ДисконтнаяКарта",
	"ДлительностьПогрузки",
	"ДоговорКонтрагента",
	"ЗаказНаВозврат",
	"Комментарий",
	"КонтактноеЛицо",
	"Контрагент",
	"КратностьВзаиморасчетов",
	"КурсВзаиморасчетов",
	"Менеджер",
	"Метки",
	"НесовместимыеМетки",
	"НомерВходящегоДокумента",
	"Организация",
	"Ответственный",
	"ОтражатьВБухгалтерскомУчете",
	"ОтражатьВНалоговомУчете",
	"ОтражатьВУправленческомУчете",
	"ОтразитьВКнигеПокупок",
	"Подразделение",
	"Проект",*/
	"Сделка" as "Сделка док. возврата",
	/*"СкладОрдер",
	"СуммаВключаетНДС",
	"СуммаДокумента",
	"СчетУчетаНДС",
	"СчетУчетаРасчетовПоАвансам",
	"СчетУчетаРасчетовПоТаре",
	"СчетУчетаРасчетовСКонтрагентом",
	"ТелефонКонтактногоЛица",
	"ТипЦен",
	"ТорговыйОбъект",
	"УсловиеПродаж",
	"УчитыватьНДС",
	"юи_СерияБСО",
	"юи_ТипБСО",
	"Проведен",*/
	"Ссылка" as "Документ возврата",
	/*"ПометкаУдаления",
	"Дата",
	"Номер",
	"ПараметрДата",
	"ВалютаДокументаГуид",
	"ДисконтнаяКартаГуид",
	"ДоговорКонтрагентаГуид",
	"ЗаказНаВозвратГуид",
	"КонтрагентГуид",
	"МенеджерГуид",
	"ОрганизацияГуид",
	"ОтветственныйГуид",
	"ПодразделениеГуид",
	"ПроектГуид",*/
	"СделкаГуид",
	/*"СкладОрдерГуид",
	"СчетУчетаНДСГуид",
	"ТипЦенГуид",*/
	"ТорговыйОбъектГуид",
	/*"УсловиеПродажГуид",
	"юи_ТипБСОГуид"*/
	"СсылкаГуид"
	--"__Partition"
from source
)

select * from renamed
