WITH stg_products AS (
    SELECT
        CAST(id AS INTEGER)                    AS product_id,
        CAST(cost AS FLOAT)                    AS cost,
        CAST(category AS VARCHAR)              AS category,
        CAST(name AS VARCHAR)                  AS name,
        CAST(brand AS VARCHAR)                 AS brand,
        CAST(retail_price AS FLOAT)            AS retail_price,
        CAST(department AS VARCHAR)            AS department,
        CAST(sku AS VARCHAR)                   AS sku,
        CAST(distribution_center_id AS INTEGER) AS distribution_center_id
    FROM {{ source('ecommerce', 'products') }}
)

SELECT * FROM stg_products
