WITH 
transponse AS (
    SELECT 
        "Гуид контрагента"::text
        {% for property in dbt_utils.get_column_values(table=ref('base_1SUPP__client_props'), column='Свойство') %}
            ,CASE
                WHEN "Свойство" = '{{property}}' THEN "Значение" 
                ELSE NULL 
            END AS "{{property}}"
        {% endfor %}
    FROM {{ ref("base_1SUPP__client_props") }}
),

deduplicated AS
(
    select 
    "Гуид контрагента"
    {% for property in dbt_utils.get_column_values(table=ref('base_1SUPP__client_props'), column='Свойство') %}
            ,MAX("{{property}}") AS "{{property}}{{' контрагента'}}"
    {% endfor %}
    from transponse
    group by "Гуид контрагента"
)

SELECT * 
FROM deduplicated
