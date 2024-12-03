{{ config(
    materialized='view',
) }}


with 
base as
(
SELECT 
{{star_exclude_guid(ref('fct_production_fact'), additional_excludes=["key_record"])}}
,fct_production_fact."ПродукцияГуид"
FROM 
{{ ref('fct_production_fact') }}
),

defined_props as
(
    select
    base.*
    ,{{star_exclude_guid(ref('dim_nomenclature'))}}
    from base
    left join {{ ref('dim_nomenclature') }}
            on dim_nomenclature."СсылкаГуид" = base."ПродукцияГуид"
),

enrichment_calendar as
(
    select 
    * 
    from defined_props
    left join {{ ref("dim_calendar") }}
        on dim_calendar.cdate = defined_props."Период выпуска прод."::date
)

select * from enrichment_calendar