{% macro exclude_guid(columns) %}
    {% set filtered_columns = [] %}
    
    {% for column in columns %}
        {% if 'Гуид' not in column %}
            {% do filtered_columns.append(column) %}
        {% endif %}
    {% endfor %}

    {{ return(filtered_columns) }}
{% endmacro %}
