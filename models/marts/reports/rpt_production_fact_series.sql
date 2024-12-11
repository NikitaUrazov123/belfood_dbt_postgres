with 
base as
(
    select
    {{star_exclude_guid(ref('mrt_production_fact_series'), additional_excludes=["record_id"])}}
    from {{ ref("mrt_production_fact_series") }}
    where "Вид номенклат." <> 'Материалы'
)
select * from base