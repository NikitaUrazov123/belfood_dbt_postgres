{{ config(
    materialized='table',
    tags=["dim"]
) }}


SELECT 
{{star_exclude_guid(ref('subdim_sale_docs_goods'))}}
,{{star_exclude_guid(ref('subdim_sale_docs'))}},
subdim_sale_docs."ТорговыйОбъектГуид" as "Гуид торгового объекта"


FROM 
{{ ref('subdim_sale_docs_goods') }}
left join {{ ref('subdim_sale_docs') }}
            on subdim_sale_docs_goods."СсылкаГуид"= subdim_sale_docs."СсылкаГуид"