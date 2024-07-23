-- models/order_status_distribution.sql

{{ config(
    schema='gold',
    materialized='view'
) }}

WITH fact_order_items AS (
    SELECT 
        status
    FROM {{ ref('fact_order_items') }}
)
SELECT 
    status,
    COUNT(*) AS order_count
FROM fact_order_items
GROUP BY status
