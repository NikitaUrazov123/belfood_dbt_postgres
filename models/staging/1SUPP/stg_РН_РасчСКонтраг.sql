with
source as
(
    select * from {{ source('Stage1CUpp', 'РН_ВзаиморасчетыСКонтрагентами') }}
),

signed as 
(
    SELECT 
    *
    ,concat("РегистраторГуид", "НомерСтроки") as key_record
    ,now() as updated_at
    FROM source
)

select * from signed