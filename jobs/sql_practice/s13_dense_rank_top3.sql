-- 题13 每个品类销量 Top3 商品（DENSE_RANK，注意并列不跳号）
WITH prod AS (
    SELECT category, product_id, SUM(quantity) AS qty
    FROM dwd_order_detail
    GROUP BY category, product_id
),
ranked AS (
    SELECT category, product_id, qty,
           DENSE_RANK() OVER (PARTITION BY category ORDER BY qty DESC) AS dr
    FROM prod
)
SELECT category, product_id, qty, dr
FROM ranked
WHERE dr <= 3
ORDER BY category, dr
LIMIT 18;
