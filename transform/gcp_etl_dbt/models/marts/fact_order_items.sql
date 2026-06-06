{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        on_schema_change='fail'
    )
}}

WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
    {% if is_incremental() %}
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),

orders AS (
    SELECT
        order_id,
        user_id,
        num_of_item,
        status
    FROM {{ ref('stg_orders') }}
),

products AS (
    SELECT
        product_id,
        cost,
        retail_price
    FROM {{ ref('stg_products') }}
)

SELECT
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    o.user_id,
    o.num_of_item,
    o.status,
    oi.sale_price,
    p.cost,
    p.retail_price,
    ROUND(oi.sale_price - p.cost, 2)                                    AS profit,
    ROUND((oi.sale_price - p.cost) / NULLIF(oi.sale_price, 0), 4)      AS margin,
    oi.created_at,
    {{ format_date_key('oi.created_at') }}                              AS order_date_key,
    {{ format_date_key('oi.shipped_at') }}                              AS shipped_date_key,
    {{ format_date_key('oi.delivered_at') }}                            AS delivered_date_key,
    {{ format_date_key('oi.returned_at') }}                             AS returned_date_key
FROM order_items oi
JOIN orders o   ON oi.order_id   = o.order_id
JOIN products p ON oi.product_id = p.product_id
