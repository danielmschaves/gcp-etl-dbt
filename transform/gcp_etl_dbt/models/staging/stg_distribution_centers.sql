WITH stg_distribution_centers AS (
    SELECT
        CAST(id AS INTEGER)        AS id,
        CAST(name AS VARCHAR)      AS name,
        CAST(latitude AS FLOAT)    AS latitude,
        CAST(longitude AS FLOAT)   AS longitude
    FROM {{ source('ecommerce', 'distribution_centers') }}
)

SELECT * FROM stg_distribution_centers
