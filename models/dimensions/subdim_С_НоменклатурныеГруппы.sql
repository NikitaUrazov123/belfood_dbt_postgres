{{ config(
    materialized='view',
    tags=["subdim"]
) }}

SELECT
	--`ЕдиницаХраненияОстатков`,
	--`БазоваяЕдиницаИзмерения`,
	`СтавкаНДС`,
	`Наименование`,
    `СсылкаГуид`
FROM
	{{ source('Stage1SUpp', 'С_НоменклатурныеГруппы') }}
where 
    `ПометкаУдаления` = False
and
    `ЭтоГруппа` = False