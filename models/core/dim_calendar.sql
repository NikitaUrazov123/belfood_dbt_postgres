with
base as
(
      select * from {{ ref("stg_refs__calendar2") }}
)

select * from base