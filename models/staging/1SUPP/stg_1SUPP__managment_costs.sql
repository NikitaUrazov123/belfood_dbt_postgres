with
base as 
(
    select * from {{ ref("base_1SUPP__prices") }}
),

filtred as 
(
    select * from base
    where "Тип цен уст. цен" = 'Производственная себестоимость (Упр Учет)'
),

renamed_and_cast as 
(
    select 
   	"Номер строки уст. цен" as "Номер строки упр. себес.",
	date_trunc('month', "Период уст. цен")::date as "Месяц упр. себес.",
	--"Нолменклатура уст. цен" as ,
	"Цена уст. цен" as "Себестроимость упр. себес.",
    "НоменклатураГуид",
	record_id
    from filtred
)

select * from renamed_and_cast
