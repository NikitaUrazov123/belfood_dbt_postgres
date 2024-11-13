{% snapshot snap_test %}
    {{
      config(
        target_schema='dbt_test',
        unique_key='customer_id',            
        strategy='check',
        check_cols = 'all'      
      )
    }}
    SELECT
        customer_id,
        name,
        status
    FROM
        {{ ref('test') }}

{% endsnapshot %}
