{{ config(
    materialized='view',
    tags=["dim"]
) }}

with
source as (
select * from {{ ref("stg_Д_ЗаказПокупателя") }}),

renamed as 
(
    select
    /*EDI_buyerGLN,
	EDI_buyerName,
	EDI_comment,
	EDI_contractNumber,
	EDI_currency,
	EDI_deliveryDateTimeFirst,
	EDI_destinationGLN,
	EDI_destinationName,
	EDI_documentDate,
	EDI_documentNameCode,
	EDI_documentNumber,
	EDI_documentType,
	EDI_function,
	EDI_lineQuantity,
	EDI_shipmentDate,
	EDI_stGLN,
	EDI_stName,
	EDI_sumNDS,
	EDI_sumNoNDS,
	EDI_supplierGLN,
	EDI_supplierName,
	`PDA_ИмпортОХ`,*/
	`АдресДоставки`,
	--`АдресПогрузки`,
	/*`ВалютаДокумента`,
	`ВидОперации`,
	`ВремяНапоминания`,
	`ВремяПрибытияПо`,
	`ВремяПрибытияС`,
	`ВыгуженКАП`,*/
	`Город`,
	/*`ГрузитьНеКратноУпаковкам`,
	`Грузоотправитель`,
	`Грузополучатель`,
	`ДатаОплаты`,
	`ДатаОтгрузки`,
	`ДатаСпецификации`,
	`ДисконтнаяКарта`,
	`ДлительностьРазгрузки`,
	`ДоговорКонтрагента`,
	`ДокументОснование`,
	`ДополнениеКАдресуДоставки`,
	`ЗонаОбслуживания`,
	`ИдентификаторВизита`,
	`ИтогПлановаяСебестоимость`,
	`КоличествоМоноПаллет`,
	`КоличествоСборныхПаллет`,
	`Комментарий`,
	`КомментарийEDI`,
	`КонтактноеЛицо`,
	`КонтактноеЛицоКонтрагента`,
	`Контрагент`,
	`КратностьВзаиморасчетов`,
	`Менеджер`,
	`Метки`,
	`МобильноеУстройство`,
	`НапомнитьОСобытии`,
	`НесовместимыеМетки`,
	`НомерПломбы`,
	`НомерПутевогоЛиста`,
	`НомерРейса`,
	`НомерСпецификации`,
	`Организация`,*/
	`Ответственный`,
	/*`Ответхранение`,
	`ОтражатьВБухгалтерскомУчете`,
	`ОтражатьВНалоговомУчете`,
	`Подразделение`,*/
	`ПутевойЛист`,
	`Регион`,
	`СкладГруппа`,
	/*`СозданEDI`,
	`СозданНаМобильномУстройстве`,
	`СтатусЗаказа`,
	`СтатусСборки`,
	`СтруктурнаяЕдиница`,
	`СуммаВключаетНДС`,
	`СуммаДокумента`,
	`ТелефонКонтактногоЛица`,*/
	`ТипЦен`,
	`ТорговыйОбъект`,
	/*`УсловиеПродаж`,
	`УчитыватьНДС`,
	`ФайлСтатусаОХ`,
	`ю_Собран`,
	`юи_Вес`,
	`юи_ДатаЗагрузки`,
	`юи_КосвенныйЗаказ`,
	`юи_Маршрут`,
	`юи_мКраткосрочная`,
	`юи_НомерВМаршруте`,
	`юи_Самовывоз`,
	`ОтвРуководитель`,
	`Проведен`,
	`Ссылка`,*/
	--`ПометкаУдаления`,
	/*`Дата`,
	`Номер`,
	`ПараметрДата`,
	`ГрузоотправительГуид`,
	`ГрузополучательГуид`,
	`ДисконтнаяКартаГуид`,
	`ДоговорКонтрагентаГуид`,
	`ДокументОснованиеГуид`,
	`ЗонаОбслуживанияГуид`,
	`КонтрагентГуид`,
	`МенеджерГуид`,
	`МобильноеУстройствоГуид`,
	`ОрганизацияГуид`,
	`ОтветственныйГуид`,
	`ПодразделениеГуид`,
	`ПутевойЛистГуид`,
	`РегионГуид`,
	`СкладГруппаГуид`,
	`ТипЦенГуид`,*/
	`ТорговыйОбъектГуид`,
	`СсылкаГуид`
	--`__Partition`
    from source
)

select * from renamed