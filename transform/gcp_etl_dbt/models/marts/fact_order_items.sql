{{ config(
    schema='gold',
    materialized='table'
) }}

WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

order_facts AS (
    SELECT
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        oi.sale_price,
        p.cost,
        p.retail_price,
        o.user_id,
        o.num_of_item,
        o.status,
        {{ format_date_key('o.created_at') }} AS order_date_key,
        {{ format_date_key('o.shipped_at') }} AS shipped_date_key,
        {{ format_date_key('o.delivered_at') }} AS delivered_date_key,
        {{ format_date_key('o.returned_at') }} AS returned_date_key
    FROM order_items oi
    JOIN {{ ref('dim_products') }} p ON oi.product_id = p.product_id    
    JOIN {{ ref('dim_orders') }} o ON oi.order_id = o.order_id
)

SELECT
    order_item_id,
    order_id,
    product_id,
    sale_price,
    cost,
    retail_price,
    user_id,
    num_of_item,
    status,
    order_date_key,
    shipped_date_key,
    delivered_date_key,
    returned_date_key
FROM order_facts
