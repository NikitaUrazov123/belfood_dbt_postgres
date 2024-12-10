WITH 
transponse AS (
    SELECT 
        "Гуид номенклатуры"::text
        {% for property in dbt_utils.get_column_values(table=ref('base_1SUPP__nomenclature_props'), column='Свойство') %}
            ,CASE
                WHEN "Свойство" = '{{property}}' THEN "Значение" 
                ELSE NULL 
            END AS "{{property}}"
        {% endfor %}
    FROM {{ ref("base_1SUPP__nomenclature_props") }}
),

deduplicated AS
(
    select 
    "Гуид номенклатуры"
    {% for property in dbt_utils.get_column_values(table=ref('base_1SUPP__nomenclature_props'), column='Свойство') %}
            ,MAX("{{property}}") AS "{{property}}{{' номенклат.'}}"
    {% endfor %}
    from transponse
    group by "Гуид номенклатуры"
)

SELECT * 
FROM deduplicated
