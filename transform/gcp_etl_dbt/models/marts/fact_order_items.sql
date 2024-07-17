-- models/fact_order_items.sql

WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

order_facts AS (
    SELECT
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        o.user_id,
        o.num_of_item
        {{ custom_date_format('o.created_at') }} AS order_date_key,
        {{ custom_date_format('o.shipped_at') }} AS shipped_date_key,
        {{ custom_date_format('o.delivered_at') }} AS delivered_date_key,
        {{ custom_date_format('o.returned_at') }} AS returned_date_key,
        oi.sale_price
    FROM order_items oi
    JOIN {{ ref('dim_orders') }} o ON oi.order_id = o.order_id
    JOIN {{ ref('dim_users') }} u ON o.user_id = u.user_id
    JOIN {{ ref('dim_products') }} p ON oi.product_id = p.product_id
)

SELECT
    order_item_id,
    order_id,
    product_id,
    user_id,
    num_of_item,
    d_order.date_key AS order_date_key,
    d_shipped.date_key AS shipped_date_key,
    d_delivered.date_key AS delivered_date_key,
    d_returned.date_key AS returned_date_key,
    sale_price
FROM order_facts
JOIN {{ ref('dim_date') }} d_order ON order_facts.order_date_key = d_order.date_key
LEFT JOIN {{ ref('dim_date') }} d_shipped ON order_facts.shipped_date_key = d_shipped.date_key
LEFT JOIN {{ ref('dim_date') }} d_delivered ON order_facts.delivered_date_key = d_delivered.date_key
LEFT JOIN {{ ref('dim_date') }} d_returned ON order_facts.returned_date_key = d_returned.date_key
