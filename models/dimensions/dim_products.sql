with nomenclature as 
(select * from {{ ref('С_Номенклатура') }}),

common_names as
(select * from {{ref ("Общие_наименования")}})

select * from nomenclature left join common_names
on nomenclature."СсылкаГуид" = common_names."ОбъектГуид"