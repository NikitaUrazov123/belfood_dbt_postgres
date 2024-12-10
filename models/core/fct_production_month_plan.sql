with
base as 
(
	SELECT * from {{ ref("stg_1SUPP__register_production_plans") }}
),

filtred as
(
	SELECT * from base 
	where "Сценарий плана производ." = 'Ежемесячный'
	and "Номенклатура плана производ." not like '%далить%'
)

SELECT * from filtred
