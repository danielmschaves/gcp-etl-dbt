WITH stg_order_items AS (
    SELECT
        CAST(id AS INTEGER)               AS order_item_id,
        CAST(order_id AS INTEGER)         AS order_id,
        CAST(user_id AS INTEGER)          AS user_id,
        CAST(product_id AS INTEGER)       AS product_id,
        CAST(inventory_item_id AS INTEGER) AS inventory_item_id,
        CAST(status AS VARCHAR)           AS status,
        CAST(created_at AS TIMESTAMP)     AS created_at,
        CAST(shipped_at AS TIMESTAMP)     AS shipped_at,
        CAST(delivered_at AS TIMESTAMP)   AS delivered_at,
        CAST(returned_at AS TIMESTAMP)    AS returned_at,
        CAST(sale_price AS FLOAT)         AS sale_price
    FROM {{ source('ecommerce', 'order_items') }}
)

SELECT * FROM stg_order_items
