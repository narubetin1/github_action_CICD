WITH d AS (
  SELECT
    location_id,
    sales_date,
    try_cast(running_no AS BIGINT)    AS running_no,    
    product_code,
    try_cast(total_amount AS DECIMAL(18,2)) AS total_amount
  FROM Detail
),
p AS (
  SELECT
    location_id,
    sales_date,
    try_cast(running_no AS BIGINT)    AS running_no,    
    product_code,
    try_cast(discount_amount AS DECIMAL(18,2)) AS discount_amount
  FROM Promotion
  WHERE try_cast(running_no AS BIGINT) IS NOT NULL     
)
SELECT
  d.location_id,
  d.sales_date,
  d.product_code,
  SUM(d.total_amount)                 AS total_amount,
  COALESCE(SUM(p.discount_amount),0)  AS discount_amount,
  SUM(d.total_amount) - COALESCE(SUM(p.discount_amount),0) AS net_sales_amount
FROM d
LEFT JOIN p
  ON p.location_id  = d.location_id
 AND p.sales_date   = d.sales_date
 AND p.running_no   = d.running_no
 AND p.product_code = d.product_code
GROUP BY d.location_id, d.sales_date, d.product_code;




WITH d AS (
  SELECT
    location_id,
    sales_date,
    product_code,
    SUM(total_amount) AS total_amount
  FROM Detail
  GROUP BY location_id, sales_date, product_code
),
p AS (
  SELECT
    location_id,
    sales_date,
    product_code,
    SUM(discount_amount) AS discount_amount
  FROM Promotion
  GROUP BY location_id, sales_date, product_code
)
SELECT
  d.location_id,
  d.sales_date,
  SUM(d.total_amount) - COALESCE(SUM(p.discount_amount), 0) AS net_sales_amount
FROM d
LEFT JOIN p
  ON p.location_id  = d.location_id
 AND p.sales_date   = d.sales_date
 AND p.product_code = d.product_code       
GROUP BY d.location_id, d.sales_date
ORDER BY d.location_id, d.sales_date;