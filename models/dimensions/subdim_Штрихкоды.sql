{{ config(
    tags=["subdim"]
) }}

with
base as
(
    select 
    *
    ,ROW_NUMBER() OVER (PARTITION BY "ВладелецГуид" ORDER BY "Штрихкод") AS row_num
     from {{ source('Stage1CUpp', 'РС_Штрихкоды') }}
),

unique_records as -- В УПП ошибка, для одного гуида опеределно два штрихкода, надо добавить в док
(
SELECT
    "Штрихкод"
    ,"ВладелецГуид"
FROM
    base
where row_num =1 
)

select * from unique_records
    