{{ config(
    schema='gold',
    materialized='table'
) }}

WITH all_dates AS (
  SELECT created_at AS date
  FROM {{ ref('stg_orders') }}
  UNION ALL
  SELECT created_at AS date
  FROM {{ ref('stg_order_items') }}
),
date_range AS (
  SELECT
    MIN(date) AS min_date,
    MAX(date) AS max_date
  FROM all_dates
),
filtered_dates AS (
  SELECT date
  FROM all_dates, date_range
  WHERE date BETWEEN date_range.min_date AND date_range.max_date
)
SELECT
  CONCAT(EXTRACT(YEAR FROM date)::TEXT, 
         LPAD(EXTRACT(MONTH FROM date)::TEXT, 2, '0'), 
         LPAD(EXTRACT(DAY FROM date)::TEXT, 2, '0')) AS date_key,
  date,
  EXTRACT(DAY FROM date) AS day,
  EXTRACT(MONTH FROM date) AS month,
  EXTRACT(QUARTER FROM date) AS quarter,
  EXTRACT(YEAR FROM date) AS year,
  EXTRACT(DOW FROM date) AS day_of_week
FROM filtered_dates