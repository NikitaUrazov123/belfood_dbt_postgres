{{ config(
    materialized='table',
    tags=["dim"]
) }}


SELECT 
{{star_exclude_guid(ref('subdim_return_docs_goods'))}},
{{star_exclude_guid(ref('subdim_returns'))}},
subdim_returns."ТорговыйОбъектГуид" as "Гуид торгового объекта"


FROM 
{{ ref('subdim_return_docs_goods') }}
left join {{ ref('subdim_returns') }}
            on subdim_return_docs_goods."СсылкаГуид"= subdim_returns."СсылкаГуид"