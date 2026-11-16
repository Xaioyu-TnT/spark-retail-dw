-- Q2 连续下单 ≥3 天的用户
-- 考点：日期 - 行号 = 同一组的日期，按组计数即连续天数
WITH d AS (
    SELECT DISTINCT user_id, order_date FROM dwd_order_detail
),
g AS (
    SELECT user_id, order_date,
           date_sub(order_date, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date)) AS grp
    FROM d
),
s AS (
    SELECT user_id, grp, COUNT(*) AS days,
           MIN(order_date) AS start_dt, MAX(order_date) AS end_dt
    FROM g GROUP BY user_id, grp
)
SELECT user_id, days, start_dt, end_dt
FROM s
WHERE days >= 3
ORDER BY days DESC, user_id
LIMIT 15;
