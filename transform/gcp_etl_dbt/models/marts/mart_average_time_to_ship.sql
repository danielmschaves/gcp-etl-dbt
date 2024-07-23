-- models/average_time_to_ship.sql

{{ config(
    schema='gold',
    materialized='view'
) }}

WITH fact_order_items AS (
    SELECT 
        order_date_key,
        shipped_date_key
    FROM {{ ref('fact_order_items') }}
),
dim_date AS (
    SELECT 
        date_key,
        date
    FROM {{ ref('dim_date') }}
)
SELECT 
    AVG(CAST(dd_shipped.date AS DATE) - CAST(dd_order.date AS DATE)) AS avg_ship_time
FROM fact_order_items foi
JOIN dim_date dd_order ON foi.order_date_key = dd_order.date_key
JOIN dim_date dd_shipped ON foi.shipped_date_key = dd_shipped.date_key
