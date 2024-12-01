{{ config(
    tags=["dim"]
) }}

with
source as
(
 select * from {{ ref('stg_Д_КорКачТоваров') }}
)

select * from source