with
source as
(
    select * from {{ source('Stage1CUpp', 'Д_ЗаказПокупателяТовары') }}
),

signed as 
(
    SELECT 
    *
    ,concat("СсылкаГуид", "НомерСтроки") as key_record
    ,now() as updated_at
    FROM source
)

select * from signed
