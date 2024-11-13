{% snapshot snap_stg_D_ZakazPokupatelyaTovary %}
    {{
      config(
        target_schema='dbt_snapshots',
        unique_key='key_record',
        strategy='timestamp',
        updated_at='"Дата"'
      )
    }}

    SELECT 
    *
    FROM {{ ref("stg_Д_ЗаказПокупателяТовары") }}

{% endsnapshot %}
