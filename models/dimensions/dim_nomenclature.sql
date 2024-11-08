{{ config(
    materialized='table',
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
/*
nomenclature_groups as
(select * from {{ ref("subdim_С_НоменклатурныеГруппы") }}),
*/
volume as 
(select* from {{ ref("subdim_Литраж") }}),

joined as
(select * from nomenclature 
left join common_names on nomenclature."СсылкаГуид" = common_names."ОбъектГуид"
left join bar_codes on nomenclature."СсылкаГуид" = bar_codes."ВладелецГуид"
--left join nomenclature_groups on nomenclature."НоменклатурнаяГруппаГуид" = nomenclature_groups."СсылкаГуид"
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
nomenclature."Артикул" as "Артикул",
/*nomenclature."БазоваяЕдиницаИзмерения" as "Единица Измерения Номенклатуры",
nomenclature."Весовой" as "Весовой",
nomenclature."ВидПроизводства" as "ВидПроизводства",*/
nomenclature."ВидНоменклатуры" as "Вид номенклатуры",
/*nomenclature."ЕдиницаДляОтчетов" as "ЕдиницаДляОтчетов",
nomenclature."ЕдиницаХраненияОстатков" as "ЕдиницаХраненияОстатков",*/
nomenclature."НаименованиеПолное" as "Полное наименование номенклатуры",
nomenclature."НоменклатурнаяГруппа" as "Номенклатурная группа",
nomenclature."НоменклатурнаяГруппаЗатрат" as "Номенклатурная группа затрат",
nomenclature."ОсновнойПоставщик" as "Основной поставщик номенклатуры",
nomenclature."СтавкаНДС" as "Ставка НДС номенклатуры",
nomenclature."СтатьяЗатрат" as "Статья затрат номенклатуры",
/*nomenclature."СтранаПроисхождения" as "СтранаПроисхождения",
nomenclature."ТребуетсяВнешняяСертификация" as "ТребуетсяВнешняяСертификация",
nomenclature."ТребуетсяВнутренняяСертификация" as "ТребуетсяВнутренняяСертификация",
nomenclature."Услуга" as "Услуга",*/
nomenclature."СрокГодности" as "Срок годности номенклатуры",
nomenclature."ВидУпаковки" as "Вид Упаковки",
--nomenclature."СоответствиеТНПА" as "СоответствиеТНПА",
nomenclature."КоличествоВУпаковке" as "Количество в упаковке номенклатуры",
nomenclature."КоличествоНаПаллете" as "Количество на паллете номенклатуры",
nomenclature."юи_ВысотаНоменклатуры" as "Высота номенклатуры",
nomenclature."СрокГодностиМесяцев" as "Срок Годности месяцев номенклатуры",
nomenclature."юи_мВидПаллетыРазмещения" as "Вид паллеты размещения номенклатуры",
nomenclature."юи_мВыдержкаДней" as "Выдержка дней номенклатуры",
nomenclature."юи_НоменклатурнаяСубгруппа" as "Номенклатурная субгруппа",
--nomenclature."ВесЕдиницы" as "ВесЕдиницы",
nomenclature."Статус" as "Статус номенклатуры",
--nomenclature."Ссылка" as "Ссылка",
nomenclature."Наименование" as "Наименование номенклатуры",
--nomenclature."Код" as "Код",
nomenclature."СсылкаГуид" as "СсылкаГуид",
common_names."Значение" as "Общее наименование номенклатуры",
bar_codes."Штрихкод" as "Штрихкод номенклатуры"
--, nomenclature_groups."Наименование" as "Номенклатурная группа"
, m_taste."Значение" as "Вкус номенклатуры"
, m_age."Значение" as "Категория Возраст номенклатуры"
, m_component."Значение" as "Компонент номенклатуры"
, m_prod_category."Значение" as "Категория продукта номенклатуры"
, package_type."Значение" as "Вид упаковки номенклатуры"
, brand."Значение" as "Бренд номенклатуры"
, bud_category."Значение" as "Вид ДП номенклатуры"
, report_group."Значение" as "Направление продукта номенклатуры"
, prod_type."Значение" as "Тип продукции номенклатуры"
, volume."Значение" as "Литраж номенклатуры"
from nomenclature 
left join common_names on nomenclature."СсылкаГуид" = common_names."ОбъектГуид"
left join bar_codes on nomenclature."СсылкаГуид" = bar_codes."ВладелецГуид"
--left join nomenclature_groups on nomenclature."НоменклатурнаяГруппаГуид" = nomenclature_groups."СсылкаГуид"
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
)

select * from renamed