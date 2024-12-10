with 
base as
(
    select
    {{star_exclude_guid(ref('mrt_production_plan_fact'), additional_excludes=["month_prod_key"])}}
    from {{ ref("mrt_production_plan_fact") }}
),

enrichment_current_month_progress as
(
    select * from base
    cross join {{ ref("current_month_passing") }}
)

select * from enrichment_current_month_progress