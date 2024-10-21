with nomenclature as 
(select * from {{ ref("stg_С_Номенклатура") }}),

common_names as
(select * from {{ref ("dim_Общие_наименования")}}),

joined as
(select * from nomenclature left join common_names
on nomenclature."СсылкаГуид" = common_names."ОбъектГуид"),

filtred as 
(select * from joined where `ПометкаУдаления` = false),

renamed as (
    select
    `Артикул`,
	`БазоваяЕдиницаИзмерения`,
	`Весовой`,
	`ВесовойКоэффициентВхождения`,
	`ВестиОперативныйУчетОстатковНЗП`,
	`ВестиПартионныйУчетПоСериям`,
	`ВестиУчетПоСериям`,
	`ВестиУчетПоСериямВНЗП`,
	`ВестиУчетПоХарактеристикам`,
	`ВидВоспроизводства`,
	`ВидНоменклатуры`,
	`ЕдиницаДляОтчетов`,
	`ЕдиницаХраненияОстатков`,
	`Комментарий`,
	`Набор`,
	`НазначениеИспользования`,
	`НаименованиеПолное`,
	`НоменклатурнаяГруппа`,
	`НоменклатурнаяГруппаЗатрат`,
	`НомерГТД`,
	`ОсновноеИзображение`,
	`ОсновнойПоставщик`,
	`ОтветственныйМенеджерЗаПокупки`,
	`СтавкаНДС`,
	`СтатьяЗатрат`,
	`СтранаПроисхождения`,
	`ТребуетсяВнешняяСертификация`,
	`ТребуетсяВнутренняяСертификация`,
	`Услуга`,
	`ВестиСерийныеНомера`,
	`Комплект`,
	`НаправлениеВыпуска`,
	`ЦеноваяГруппа`,
	`ОКП`,
	`ЕдиницаИзмеренияМест`,
	`Сертификат`,
	`УдостоверениеГГР`,
	`СрокГодности`,
	`ВидУпаковки`,
	`СоответствиеТНПА`,
	`КоличествоВУпаковке`,
	`КоличествоНаПаллете`,
	`юи_ВысотаНоменклатуры`,
	`АртикулСтараяКрепость`,
	`СрокГодностиМесяцев`,
	`юи_мВидПаллетыРазмещения`,
	`юи_мРасчитыватьСрокПоДням`,
	`юи_мВыдержкаДней`,
	`юи_БриксМин`,
	`юи_БриксМакс`,
	`юи_КислотностьМин`,
	`юи_КислотностьМакс`,
	`юи_Плотность`,
	`юи_ТочностьОкругления`,
	`юи_ОсновнаяПартияРозлива`,
	`юи_УчитыватьПотери`,
	`юи_Код77`,
	`юи_Андроид`,
	`юи_КодТНВЭД`,
	`ДекларацияТРТС`,
	`юи_НоменклатурнаяСубгруппа`,
	`юи_ВидТары`,
	`юи_ШтукВЕдТары`,
	`юи_НоменклатурнаяГруппаОПС`,
	`ВидОборудования`,
	`юи_ГруппаДляТТН`,
	`ВесЕдиницы`,
	`Статус`,
	`Предопределенный`,
	`Ссылка`,
	`ПометкаУдаления`,
	`ЭтоГруппа`,
	`Родитель`,
	`Наименование`,
	`Код`,
	`ПараметрНаименование`,
	`common_names.Значение` as "Общее наименование"
    from filtred
)

select * from renamed