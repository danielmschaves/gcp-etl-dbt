WITH stg_orders AS (
    SELECT
        CAST(order_id AS INTEGER)     AS order_id,
        CAST(user_id AS INTEGER)      AS user_id,
        CAST(status AS VARCHAR)       AS status,
        CAST(gender AS VARCHAR)       AS gender,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(returned_at AS TIMESTAMP) AS returned_at,
        CAST(shipped_at AS TIMESTAMP) AS shipped_at,
        CAST(delivered_at AS TIMESTAMP) AS delivered_at,
        CAST(num_of_item AS INTEGER)  AS num_of_item
    FROM {{ source('ecommerce', 'orders') }}
)

SELECT * FROM stg_orders
