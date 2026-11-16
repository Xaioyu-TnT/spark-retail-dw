-- Q1 每个品类销售额前三的商品
-- 考点：窗口函数 ROW_NUMBER/RANK 的 PARTITION BY + 子查询过滤
WITH prod_gmv AS (
    SELECT category, product_id, SUM(amount) AS gmv
    FROM dwd_order_detail
    GROUP BY category, product_id
),
ranked AS (
    SELECT category, product_id, gmv,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY gmv DESC) AS rn
    FROM prod_gmv
)
SELECT category, rn, product_id, ROUND(gmv, 2) AS gmv
FROM ranked
WHERE rn <= 3
ORDER BY category, rn;
