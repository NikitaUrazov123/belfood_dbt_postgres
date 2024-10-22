with nomenclature as 
(select * from {{ ref("stg_С_Номенклатура") }}),

common_names as
(select * from {{ref ("subdim_Общие_наименования")}}),

bar_codes as 
(select * from {{ref('subdim_Штрихкоды')}}),

joined as
(select * from nomenclature 
left join common_names on nomenclature."СсылкаГуид" = common_names."ОбъектГуид"
left join bar_codes on nomenclature."СсылкаГуид" = bar_codes."ВладелецГуид"),

filtred as 
(select * from joined 
	where 
		nomenclature.`ПометкаУдаления` = false
	and 
		nomenclature.`ЭтоГруппа` = false),

renamed as (
select
nomenclature.`Артикул` as "Артикул",
nomenclature.`БазоваяЕдиницаИзмерения` as "БазоваяЕдиницаИзмерения",
nomenclature.`Весовой` as "Весовой",
nomenclature.`ВесовойКоэффициентВхождения` as "ВесовойКоэффициентВхождения",
nomenclature.`ВестиОперативныйУчетОстатковНЗП` as "ВестиОперативныйУчетОстатковНЗП",
nomenclature.`ВестиПартионныйУчетПоСериям` as "ВестиПартионныйУчетПоСериям",
nomenclature.`ВестиУчетПоСериям` as "ВестиУчетПоСериям",
nomenclature.`ВестиУчетПоСериямВНЗП` as "ВестиУчетПоСериямВНЗП",
nomenclature.`ВестиУчетПоХарактеристикам` as "ВестиУчетПоХарактеристикам",
nomenclature.`ВидВоспроизводства` as "ВидВоспроизводства",
nomenclature.`ВидНоменклатуры` as "ВидНоменклатуры",
nomenclature.`ЕдиницаДляОтчетов` as "ЕдиницаДляОтчетов",
nomenclature.`ЕдиницаХраненияОстатков` as "ЕдиницаХраненияОстатков",
nomenclature.`Комментарий` as "Комментарий",
nomenclature.`Набор` as "Набор",
nomenclature.`НазначениеИспользования` as "НазначениеИспользования",
nomenclature.`НаименованиеПолное` as "НаименованиеПолное",
nomenclature.`НоменклатурнаяГруппа` as "НоменклатурнаяГруппа",
nomenclature.`НоменклатурнаяГруппаЗатрат` as "НоменклатурнаяГруппаЗатрат",
nomenclature.`НомерГТД` as "НомерГТД",
nomenclature.`ОсновноеИзображение` as "ОсновноеИзображение",
nomenclature.`ОсновнойПоставщик` as "ОсновнойПоставщик",
nomenclature.`ОтветственныйМенеджерЗаПокупки` as "ОтветственныйМенеджерЗаПокупки",
nomenclature.`СтавкаНДС` as "СтавкаНДС",
nomenclature.`СтатьяЗатрат` as "СтатьяЗатрат",
nomenclature.`СтранаПроисхождения` as "СтранаПроисхождения",
nomenclature.`ТребуетсяВнешняяСертификация` as "ТребуетсяВнешняяСертификация",
nomenclature.`ТребуетсяВнутренняяСертификация` as "ТребуетсяВнутренняяСертификация",
nomenclature.`Услуга` as "Услуга",
nomenclature.`ВестиСерийныеНомера` as "ВестиСерийныеНомера",
nomenclature.`Комплект` as "Комплект",
nomenclature.`НаправлениеВыпуска` as "НаправлениеВыпуска",
nomenclature.`ЦеноваяГруппа` as "ЦеноваяГруппа",
nomenclature.`ОКП` as "ОКП",
nomenclature.`ЕдиницаИзмеренияМест` as "ЕдиницаИзмеренияМест",
nomenclature.`Сертификат` as "Сертификат",
nomenclature.`УдостоверениеГГР` as "УдостоверениеГГР",
nomenclature.`СрокГодности` as "СрокГодности",
nomenclature.`ВидУпаковки` as "ВидУпаковки",
nomenclature.`СоответствиеТНПА` as "СоответствиеТНПА",
nomenclature.`КоличествоВУпаковке` as "КоличествоВУпаковке",
nomenclature.`КоличествоНаПаллете` as "КоличествоНаПаллете",
nomenclature.`юи_ВысотаНоменклатуры` as "юи_ВысотаНоменклатуры",
nomenclature.`АртикулСтараяКрепость` as "АртикулСтараяКрепость",
nomenclature.`СрокГодностиМесяцев` as "СрокГодностиМесяцев",
nomenclature.`юи_мВидПаллетыРазмещения` as "юи_мВидПаллетыРазмещения",
nomenclature.`юи_мРасчитыватьСрокПоДням` as "юи_мРасчитыватьСрокПоДням",
nomenclature.`юи_мВыдержкаДней` as "юи_мВыдержкаДней",
nomenclature.`юи_БриксМин` as "юи_БриксМин",
nomenclature.`юи_БриксМакс` as "юи_БриксМакс",
nomenclature.`юи_КислотностьМин` as "юи_КислотностьМин",
nomenclature.`юи_КислотностьМакс` as "юи_КислотностьМакс",
nomenclature.`юи_Плотность` as "юи_Плотность",
nomenclature.`юи_ТочностьОкругления` as "юи_ТочностьОкругления",
nomenclature.`юи_ОсновнаяПартияРозлива` as "юи_ОсновнаяПартияРозлива",
nomenclature.`юи_УчитыватьПотери` as "юи_УчитыватьПотери",
nomenclature.`юи_Код77` as "юи_Код77",
nomenclature.`юи_Андроид` as "юи_Андроид",
nomenclature.`юи_КодТНВЭД` as "юи_КодТНВЭД",
nomenclature.`ДекларацияТРТС` as "ДекларацияТРТС",
nomenclature.`юи_НоменклатурнаяСубгруппа` as "юи_НоменклатурнаяСубгруппа",
nomenclature.`юи_ВидТары` as "юи_ВидТары",
nomenclature.`юи_ШтукВЕдТары` as "юи_ШтукВЕдТары",
nomenclature.`юи_НоменклатурнаяГруппаОПС` as "юи_НоменклатурнаяГруппаОПС",
nomenclature.`ВидОборудования` as "ВидОборудования",
nomenclature.`юи_ГруппаДляТТН` as "юи_ГруппаДляТТН",
nomenclature.`ВесЕдиницы` as "ВесЕдиницы",
nomenclature.`Статус` as "Статус",
nomenclature.`Предопределенный` as "Предопределенный",
nomenclature.`Ссылка` as "Ссылка",
nomenclature.`ПометкаУдаления` as "ПометкаУдаления",
nomenclature.`ЭтоГруппа` as "ЭтоГруппа",
nomenclature.`Родитель` as "Родитель",
nomenclature.`Наименование` as "Наименование",
nomenclature.`Код` as "Код",
nomenclature.`ПараметрНаименование` as "ПараметрНаименование",
common_names.`Значение` as "Общее наименование",
bar_codes.`Штрихкод` as "Штрихкод"
from filtred
)

select * from renamed