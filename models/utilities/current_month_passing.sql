{{
    config(
        materialized='view'
    )
}}

WITH 
    remains_days AS (
        SELECT 
            generate_series(CURRENT_DATE, date_trunc('month', CURRENT_DATE) + interval '1 month' - interval '1 day', '1 day')::date AS date
    ),
    entire_month AS (
        SELECT 
            generate_series(date_trunc('month', CURRENT_DATE), date_trunc('month', CURRENT_DATE) + interval '1 month' - interval '1 day', '1 day')::date AS date
    ),
    data_union AS (
        SELECT
            COUNT(em.date) AS total_days_in_month,
            NULL::int AS remaining_days_in_month,
            NULL::int AS total_workdays_in_month,
            NULL::int AS remaining_workdays_in_month
        FROM entire_month em
        
        UNION ALL

        SELECT
            NULL::int AS total_days_in_month,
            COUNT(rd.date) AS remaining_days_in_month,
            NULL::int AS total_workdays_in_month,
            NULL::int AS remaining_workdays_in_month
        FROM remains_days rd

        UNION ALL

        SELECT
            NULL::int AS total_days_in_month,
            NULL::int AS remaining_days_in_month,
            COUNT(em.date) AS total_workdays_in_month,
            NULL::int AS remaining_workdays_in_month
        FROM entire_month em
        WHERE EXTRACT(ISODOW FROM em.date) BETWEEN 1 AND 5

        UNION ALL

        SELECT
            NULL::int AS total_days_in_month,
            NULL::int AS remaining_days_in_month,
            NULL::int AS total_workdays_in_month,
            COUNT(rd.date) AS remaining_workdays_in_month
        FROM remains_days rd
        WHERE EXTRACT(ISODOW FROM rd.date) BETWEEN 1 AND 5
    )
SELECT
    SUM(total_days_in_month) AS total_days_in_month,
    SUM(remaining_days_in_month) AS remaining_days_in_month,
    SUM(total_workdays_in_month) AS total_workdays_in_month,
    SUM(remaining_workdays_in_month) AS remaining_workdays_in_month,
    1 - (SUM(remaining_workdays_in_month)::float / NULLIF(SUM(total_workdays_in_month), 0)) AS percentage_workdays_passed,
    1 - (SUM(remaining_days_in_month)::float / NULLIF(SUM(total_days_in_month), 0)) AS percentage_days_passed
FROM data_union
