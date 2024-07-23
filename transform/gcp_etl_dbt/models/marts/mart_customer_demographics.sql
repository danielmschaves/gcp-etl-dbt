-- models/customer_demographics.sql
{{ config(
    schema='gold',
    materialized='view'
) }}

WITH fact_order_items AS (
    SELECT 
        user_id
    FROM {{ ref('fact_order_items') }}
),
dim_users AS (
    SELECT 
        user_id,
        gender
    FROM {{ ref('dim_users') }}
)
SELECT 
    du.gender,
    COUNT(DISTINCT foi.user_id) AS user_count
FROM fact_order_items foi
JOIN dim_users du ON foi.user_id = du.user_id
GROUP BY du.gender
