-- 题8 每个商品的 GMV 占其品类 GMV 的百分比（窗口 + PARTITION BY）
WITH prod AS (
    SELECT category, product_id, SUM(amount) AS gmv
    FROM dwd_order_detail
    GROUP BY category, product_id
)
SELECT category, product_id,
       ROUND(gmv, 2)                                                AS gmv,
       ROUND(gmv / SUM(gmv) OVER (PARTITION BY category) * 100, 2)   AS pct_in_cat
FROM prod
ORDER BY category, pct_in_cat DESC
LIMIT 15;
