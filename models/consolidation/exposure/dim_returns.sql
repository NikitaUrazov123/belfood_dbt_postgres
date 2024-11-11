{{ config(
    tags=["dim"]
) }}


SELECT 
{{star_exclude_guid(ref('subdim_return_docs_goods'))}},
{{star_exclude_guid(ref('subdim_returns'))}}


FROM 
{{ ref('subdim_return_docs_goods') }}
left join {{ ref('subdim_returns') }}
            on subdim_return_docs_goods."СсылкаГуид"= subdim_returns."СсылкаГуид"