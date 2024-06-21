WITH customers AS (
  SELECT
    DISTINCT oi.user_id,
    SUM(CASE WHEN u.gender = 'M' THEN 1 ELSE 0 END) AS male,
    SUM(CASE WHEN u.gender = 'F' THEN 1 ELSE 0 END) AS female,
    u.country AS country
  FROM stg_order_items AS oi
  INNER JOIN stg_users AS u ON oi.user_id = u.id
  WHERE oi.status NOT IN ('Cancelled', 'Returned')
  GROUP BY oi.user_id, u.country
)

SELECT
  country,
  COUNT(DISTINCT user_id) AS customers_count,
  SUM(female) AS female,
  SUM(male) AS male
FROM customers
GROUP BY country
ORDER BY customers_count DESC;