-- models/top_customers.sql

{{ config(
    schema='gold',
    materialized='view'
) }}

WITH fact_order_items AS (
    SELECT 
        user_id,
        sale_price
    FROM {{ ref('fact_order_items') }}
)
SELECT 
    user_id,
    SUM(sale_price) AS total_revenue
FROM fact_order_items
GROUP BY user_id
ORDER BY total_revenue DESC
LIMIT 10