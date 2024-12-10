with
base as
(
      select * from {{ ref("stg_1SUPP__С_Качество") }}
)

select * from base