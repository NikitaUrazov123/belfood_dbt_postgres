with
base as
(
    select * from {{ ref("base_1SUPP__barcodes") }}
),

renamed_and_cast as
(
    select
    "Штрихкод",
    "ВладелецГуид"::text
    from base
)

select * from renamed_and_cast