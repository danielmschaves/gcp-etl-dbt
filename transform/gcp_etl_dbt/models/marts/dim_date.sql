{{ config(
    schema='gold',
    materialized='table'
) }}


WITH all_dates AS (
  SELECT CAST(created_at AS DATE) AS date
  FROM {{ ref('stg_orders') }}
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
),
date_keys AS (
  SELECT
    {{ format_date_key('date') }} AS date_key,
    date,
    EXTRACT(DAY FROM date) AS day,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(QUARTER FROM date) AS quarter,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(DOW FROM date) AS day_of_week
  FROM filtered_dates
)

SELECT
  DISTINCT(date_key) as date_key,
  date,
  day,
  month,
  quarter,
  year,
  day_of_week
FROM date_keys