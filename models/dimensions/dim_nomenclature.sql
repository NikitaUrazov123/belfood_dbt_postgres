{{ config(
    materialized='table',
    tags=["dim"]
) }}

with nomenclature as 
(select * from {{ ref("stg_С_Номенклатура") }}),

common_names as
(select * from {{ref ("subdim_Общие_наименования")}}),

m_taste as
(select * from {{ ref("subdim_мВкус") }}),

m_age as
(select * from {{ ref("subdim_мКатегорияВозраст") }}),

m_component as 
(select * from {{ ref("subdim_мКатегорияКомпонент") }}),

m_prod_category as 
(select * from {{ ref("subdim_мКатегорияПродукт") }}),

prod_type as
(select * from {{ ref("subdim_ТипПродукции") }}),

report_group as 
(select * from {{ ref("subdim_ГруппаТоваровДляОтчета") }}),

brand as 
(select * from {{ ref("subdim_Бренд") }}),

bud_category as
(select * from {{ ref("subdim_ВидДП") }}),

package_type as 
(select * from {{ ref("subdim_ВидУпаковки") }}),

bar_codes as 
(select * from {{ref('subdim_Штрихкоды')}}),

nomenclature_groups as
(select * from {{ ref("subdim_С_НоменклатурныеГруппы") }}),

volume as 
(select* from {{ ref("subdim_Литраж") }}),

joined as
(select * from nomenclature 
left join common_names on nomenclature."СсылкаГуид" = common_names."ОбъектГуид"
left join bar_codes on nomenclature."СсылкаГуид" = bar_codes."ВладелецГуид"
left join nomenclature_groups on nomenclature."НоменклатурнаяГруппаГуид" = nomenclature_groups."СсылкаГуид"
left join m_taste on nomenclature."СсылкаГуид" = m_taste."ОбъектГуид"
left join m_age on nomenclature."СсылкаГуид" = m_age."ОбъектГуид"
left join m_component on nomenclature."СсылкаГуид" = m_component."ОбъектГуид"
left join m_prod_category on nomenclature."СсылкаГуид" = m_prod_category."ОбъектГуид"
left join package_type on nomenclature."СсылкаГуид" = package_type."ОбъектГуид"
left join brand on nomenclature."СсылкаГуид" = brand."ОбъектГуид"
left join bud_category on nomenclature."СсылкаГуид" = bud_category."ОбъектГуид"
left join prod_type on nomenclature."СсылкаГуид" = prod_type."ОбъектГуид"
left join report_group on nomenclature."СсылкаГуид" = report_group."ОбъектГуид"
left join volume on nomenclature."СсылкаГуид" = volume."ОбъектГуид"
),

filtred as 
(select * from joined 
	where 
		nomenclature.`ПометкаУдаления` = False
	and 
		nomenclature.`ЭтоГруппа` = False),

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
common_names.`Значение` as "Общее наименование",
bar_codes.`Штрихкод` as "Штрихкод"
, nomenclature_groups.`Наименование` as "Номенклатурная группа"
, m_taste.`Значение` as "Вкус"
, m_age.`Значение` as "Категория Возраст"
, m_component.`Значение` as "Компонент"
, m_prod_category.`Значение` as "Категория Продукт"
, package_type.`Значение` as "Вид упаковки"
, brand.`Значение` as "Бренд"
, bud_category.`Значение` as "Вид ДП"
, report_group.`Значение` as "Направление продукта"
, prod_type.`Значение` as "Тип продукции"
, volume.`Значение` as "Литраж"
from filtred
)

select * from renamed