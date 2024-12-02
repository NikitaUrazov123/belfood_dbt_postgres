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

type_of_production as 
(select * from {{ ref("subdim_Вид_производства") }}),

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

fac_conv_to_liters as 
(
    select * from {{ ref("subdim_Коэф_номенклатуры_в_литры") }}
),

renamed as (
select
nomenclature."Артикул" as "Артикул",
/*nomenclature."БазоваяЕдиницаИзмерения" as "Единица Измерения номенклат.",
nomenclature."Весовой" as "Весовой",
nomenclature."ВидПроизводства" as "ВидПроизводства",*/
nomenclature."ВидНоменклатуры" as "Вид номенклат.",
/*nomenclature."ЕдиницаДляОтчетов" as "ЕдиницаДляОтчетов",
nomenclature."ЕдиницаХраненияОстатков" as "ЕдиницаХраненияОстатков",*/
nomenclature."НаименованиеПолное" as "Полное наименование номенклат.",
nomenclature."НоменклатурнаяГруппа" as "Номенклатурная группа",
--nomenclature."НоменклатурнаяГруппаЗатрат" as "Номенклатурная группа затрат",
nomenclature."ОсновнойПоставщик" as "Основной поставщик номенклат.",
nomenclature."СтавкаНДС" as "Ставка НДС номенклат.",
--nomenclature."СтатьяЗатрат" as "Статья затрат номенклат.",
/*nomenclature."СтранаПроисхождения" as "СтранаПроисхождения",
nomenclature."ТребуетсяВнешняяСертификация" as "ТребуетсяВнешняяСертификация",
nomenclature."ТребуетсяВнутренняяСертификация" as "ТребуетсяВнутренняяСертификация",
nomenclature."Услуга" as "Услуга",*/
nomenclature."СрокГодности" as "Срок годности номенклат.",
nomenclature."ВидУпаковки" as "Вид Упаковки",
--nomenclature."СоответствиеТНПА" as "СоответствиеТНПА",
nomenclature."КоличествоВУпаковке" as "Количество в упаковке номенклат.",
nomenclature."КоличествоНаПаллете" as "Количество на паллете номенклат.",
nomenclature."юи_ВысотаНоменклатуры" as "Высота номенклат.",
--nomenclature."СрокГодностиМесяцев" as "Срок Годности месяцев номенклат.",
nomenclature."юи_мВидПаллетыРазмещения" as "Вид паллеты размещения номенклат.",
nomenclature."юи_мВыдержкаДней" as "Выдержка дней номенклат.",
--nomenclature."юи_НоменклатурнаяСубгруппа" as "Номенклатурная субгруппа",
--nomenclature."ВесЕдиницы" as "ВесЕдиницы",
nomenclature."Статус" as "Статус номенклат.",
--nomenclature."Ссылка" as "Ссылка",
nomenclature."Наименование" as "Наименование номенклат.",
--nomenclature."Код" as "Код",
nomenclature."СсылкаГуид" as "СсылкаГуид",
common_names."Значение" as "Общее наименование номенклат.",
bar_codes."Штрихкод" as "Штрихкод номенклат."
,nomenclature."юи_КодТНВЭД" as "Код ТНВЭД номенклат."
--, nomenclature_groups."Наименование" as "Номенклатурная группа"
, replace(trim(LOWER(m_taste."Значение")),' ', '_') as "Вкус номенклат."
, m_age."Значение" as "Категория Возраст номенклат."
, m_component."Значение" as "Компонент номенклат."
, m_prod_category."Значение" as "Категория продукта номенклат."
, package_type."Значение" as "Вид упаковки номенклат."
, brand."Значение" as "Бренд номенклат."
, bud_category."Значение" as "Вид ДП номенклат."
, type_of_production."Значение" as "Направление продукта номенклат."
, prod_type."Значение" as "Тип продукции номенклат."
, volume."Значение" as "Литраж номенклат."
,fac_conv_to_liters."Коэфициент в литры" as "Коэф. перевода в литры"
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
left join type_of_production on nomenclature."СсылкаГуид" = type_of_production."ОбъектГуид"
left join volume on nomenclature."СсылкаГуид" = volume."ОбъектГуид"
left join fac_conv_to_liters on fac_conv_to_liters."ВладелецГуид"::text = nomenclature."СсылкаГуид"::text
)

select * from renamed