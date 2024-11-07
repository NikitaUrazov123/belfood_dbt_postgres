{% macro star_exclude_guid(model_ref, exclude_substring="Гуид", additional_excludes=[]) %}
    {%- set columns = adapter.get_columns_in_relation(model_ref) -%}
    {%- set excluded_columns = [] -%}

    {%- for column in columns -%}
        {%- if exclude_substring in column.name or column.name in additional_excludes -%}
            {%- do excluded_columns.append(column.name) -%}
        {%- endif -%}
    {%- endfor -%}

    {{ dbt_utils.star(from=model_ref, except=excluded_columns) }}
{% endmacro %}
