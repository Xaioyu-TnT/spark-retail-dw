-- Q8 没有订单的日期补 0
-- 考点：生成日期序列 + LEFT JOIN + COALESCE
-- 说明：本数据集 90 天连续无缺口，所以「缺失日期」为 0 行 —— 这本身是一个数据质量结论
WITH bounds AS (
    SELECT MIN(order_date) AS min_dt, MAX(order_date) AS max_dt FROM dwd_order_detail
),
dates AS (
    SELECT explode(sequence(min_dt, max_dt, interval 1 day)) AS dt FROM bounds
),
daily AS (
    SELECT order_date, SUM(amount) AS gmv FROM dwd_order_detail GROUP BY order_date
),
filled AS (
    SELECT d.dt                            AS order_date,
           COALESCE(ROUND(x.gmv, 2), 0)    AS gmv,
           x.gmv IS NULL                   AS is_missing
    FROM dates d
    LEFT JOIN daily x ON d.dt = x.order_date
)
SELECT COUNT(*)                                       AS total_days,
       SUM(CASE WHEN is_missing THEN 1 ELSE 0 END)    AS missing_days,
       ROUND(SUM(gmv), 2)                             AS gmv_total
FROM filled;
