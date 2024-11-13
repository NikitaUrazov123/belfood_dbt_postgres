{% snapshot snap_stg_D_ZakazPokupatelya %}
    {{
      config(
        target_schema='dbt_snapshots',
        unique_key='"СсылкаГуид"',
        strategy='check',
        check_cols = 'all'     
      )
    }}
    SELECT * FROM {{ ref("stg_Д_ЗаказПокупателя") }}

{% endsnapshot %}
