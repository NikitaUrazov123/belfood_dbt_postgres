{% set currencies = ['USD', 'EUR', 'RUB'] %}

with 
source as(
SELECT *
FROM {{ source('api_nbrb_by_exrates', 'rates') }}
),

date_column as
(
  select distinct "Date" from source
),

{% for cur in currencies %}
{{cur}} as(
    SELECT
    "Date"
    ,"Cur_OfficialRate" as "{{cur}}"
    ,"Cur_Scale" as "{{cur}}{{'_scale'}}"
    from source
    where "Cur_Abbreviation" = '{{cur}}'
),
  {% endfor %}

joined as
(
  select
  date_column."Date"::date as "Date"
  {% for cur in currencies %}
  ,"{{cur}}"
  ,"{{cur}}{{'_scale'}}"{% endfor %}
  from date_column
  {% for cur in currencies %}
  left join {{cur}} on {{cur}}."Date" = date_column."Date"
  {% endfor %}

)

select * from joined