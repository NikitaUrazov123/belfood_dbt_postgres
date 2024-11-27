{{ config(
    tags=["dim"]
) }}

with
source as
(
 select * from {{ ref("stg_Д_КорректировкаКачестваТоваров") }}
)

select * from source