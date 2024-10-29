{{ config(
    materialized='view',
    tags=["dim"]
) }}

with nomenclature as 
(SELECT * FROM {{ ref("stg_С_Номенклатура") }}),

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

renamed as (
select
nomenclature.`Артикул` as "Артикул",
/*nomenclature.`БазоваяЕдиницаИзмерения` as "Единица Измерения Номенклатуры",
nomenclature.`Весовой` as "Весовой",
nomenclature.`ВидПроизводства` as "ВидПроизводства",*/
nomenclature.`ВидНоменклатуры` as "ВидНоменклатуры",
/*nomenclature.`ЕдиницаДляОтчетов` as "ЕдиницаДляОтчетов",
nomenclature.`ЕдиницаХраненияОстатков` as "ЕдиницаХраненияОстатков",*/
nomenclature.`НаименованиеПолное` as "НаименованиеПолное",
nomenclature.`НоменклатурнаяГруппа` as "Номенклатурная Группа",
nomenclature.`НоменклатурнаяГруппаЗатрат` as "Номенклатурная Группа Затрат",
nomenclature.`ОсновнойПоставщик` as "Основной Поставщик Номенклатуры",
nomenclature.`СтавкаНДС` as "СтавкаНДС Номенклатуры",
nomenclature.`СтатьяЗатрат` as "СтатьяЗатрат Номенклатуры",
/*nomenclature.`СтранаПроисхождения` as "СтранаПроисхождения",
nomenclature.`ТребуетсяВнешняяСертификация` as "ТребуетсяВнешняяСертификация",
nomenclature.`ТребуетсяВнутренняяСертификация` as "ТребуетсяВнутренняяСертификация",
nomenclature.`Услуга` as "Услуга",*/
nomenclature.`СрокГодности` as "Срок Годности Номенклатуры",
nomenclature.`ВидУпаковки` as "Вид Упаковки",
--nomenclature.`СоответствиеТНПА` as "СоответствиеТНПА",
nomenclature.`КоличествоВУпаковке` as "Количество В Упаковке Номенклатуры",
nomenclature.`КоличествоНаПаллете` as "Количество На Паллете Номенклатуры",
nomenclature.`юи_ВысотаНоменклатуры` as "Высота Номенклатуры",
nomenclature.`СрокГодностиМесяцев` as "Срок Годности Месяцев Номенклатуры",
nomenclature.`юи_мВидПаллетыРазмещения` as "Вид Паллеты Размещения Номенклатуры",
nomenclature.`юи_мВыдержкаДней` as "Выдержка Дней Номенклатуры",
nomenclature.`юи_НоменклатурнаяСубгруппа` as "Номенклатурная Субгруппа",
--nomenclature.`ВесЕдиницы` as "ВесЕдиницы",
nomenclature.`Статус` as "Статус Номенклатуры",
--nomenclature.`Ссылка` as "Ссылка",
nomenclature.`Наименование` as "Наименование",
--nomenclature.`Код` as "Код",
nomenclature.`СсылкаГуид` as "СсылкаГуид",
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
from joined
)

select * from renamed