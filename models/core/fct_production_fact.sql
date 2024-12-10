with 
base as
(
	select * from {{ ref("stg_1SUPP__register_production") }}
),

filtred as
(
	select * from base
	WHERE ("Регистратор выпуска прод." LIKE '%тчет%') OR ("Регистратор выпуска прод." LIKE '%оступлен%')
)

select * from filtred