with
source as
(
    select * from {{ source('Stage1CUpp', 'РС_Штрихкоды') }}
),

counted_records_by_barcode as -- В УПП ошибка, для одного гуида опеределно два штрихкода, надо добавить в док
(
SELECT
    "Штрихкод"
    ,"ВладелецГуид"::text
    ,ROW_NUMBER() OVER (PARTITION BY "ВладелецГуид" ORDER BY "Штрихкод") AS row_num
FROM
    source
),

unique_records as
(
    select
    "Штрихкод"
    ,"ВладелецГуид"
    from counted_records_by_barcode
    where row_num = 1
)

select * from unique_records