-- models/sales_by_category.sql
{{ config(
    schema='gold',
    materialized='view'
) }}

WITH fact_order_items AS (
    SELECT 
        order_id,
        product_id,
        sale_price
    FROM {{ ref('fact_order_items') }}
),
dim_products AS (
    SELECT 
        product_id,
        category
    FROM {{ ref('dim_products') }}
)
SELECT 
    dp.category,
    SUM(foi.sale_price) AS total_sales
FROM fact_order_items foi
JOIN dim_products dp ON foi.product_id = dp.product_id
GROUP BY dp.category
ORDER BY total_sales DESC
