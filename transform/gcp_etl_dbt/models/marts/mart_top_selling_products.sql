-- models/top_selling_products.sql

{{ config(
    schema='gold',
    materialized='view'
) }}

WITH fact_order_items AS (
    SELECT 
        order_id,
        product_id,
        num_of_item
    FROM {{ ref('fact_order_items') }}
),
dim_products AS (
    SELECT 
        product_id,
        name
    FROM {{ ref('dim_products') }}
)
SELECT 
    dp.name AS product_name,
    SUM(foi.num_of_item) AS total_items_sold
FROM fact_order_items foi
JOIN dim_products dp ON foi.product_id = dp.product_id
GROUP BY dp.name
ORDER BY total_items_sold DESC
LIMIT 10
