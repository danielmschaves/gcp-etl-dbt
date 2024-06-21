-- models/dim_date.sql

WITH date_range AS (
    SELECT
        MIN(DATE(created_at)) AS min_date,
        MAX(DATE(created_at)) AS max_date
    FROM (
        SELECT created_at FROM {{ ref('stg_orders') }}
        UNION ALL
        SELECT created_at FROM {{ ref('stg_order_items') }}
    ) AS combined_dates
),

date_spine AS (
    SELECT
        DATEADD(day, seq, min_date) AS date
    FROM date_range
    JOIN (
        SELECT row_number() OVER () - 1 as seq
        FROM {{ ref('stg_orders') }},
             {{ ref('stg_order_items') }}
    ) AS sequence
    WHERE DATEADD(day, seq, min_date) <= max_date
)

SELECT
    CAST(DATE_FORMAT(date, '%Y%m%d') AS INT) AS date_key,
    date,
    DAY(date) AS day,
    MONTH(date) AS month,
    QUARTER(date) AS quarter,
    YEAR(date) AS year,
    DAYOFWEEK(date) AS day_of_week
FROM date_spine
