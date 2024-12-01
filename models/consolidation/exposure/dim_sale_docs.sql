{{ config(
    materialized='table',
    tags=["dim", "incremental"],
    unique_key='key',
    incremental_strategy='delete+insert'
) }}


SELECT 
{{star_exclude_guid(ref('subdim_sale_docs_goods'))}}
-------------------------------------------------------------------
,{{star_exclude_guid(ref('subdim_sale_docs'))}},
subdim_sale_docs."ТорговыйОбъектГуид" as "Гуид торгового объекта"
FROM 
{{ ref('subdim_sale_docs_goods') }}
left join {{ ref('subdim_sale_docs') }}
            on subdim_sale_docs_goods."СсылкаГуид"= subdim_sale_docs."СсылкаГуид"

{% if is_incremental() %}
where "Дата док. реализации"::date >= CURRENT_DATE - interval '2 months'
{% endif %}