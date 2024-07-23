-- models/sales_by_country.sql

{{ config(
    schema='gold',
    materialized='view'
) }}

WITH fact_order_items AS (
    SELECT 
        user_id,
        sale_price
    FROM {{ ref('fact_order_items') }}
),
dim_users AS (
    SELECT 
        user_id,
        country
    FROM {{ ref('dim_users') }}
)
SELECT 
    du.country,
    SUM(foi.sale_price) AS total_sales
FROM fact_order_items foi
JOIN dim_users du ON foi.user_id = du.user_id
GROUP BY du.country
ORDER BY total_sales DESC
LIMIT 10
