with
base as
(
    select * from {{ ref("base_1SUPP__barcodes") }}
),

renamed as
(
    select
    "Штрихкод",
    "ВладелецГуид"::text
    from base
)

select * from renamed