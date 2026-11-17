-- 题12 复购间隔：相邻两次下单的天数差（LAG + DATEDIFF）
WITH user_days AS (
    SELECT DISTINCT user_id, order_date FROM dwd_order_detail
),
lagged AS (
    SELECT user_id, order_date,
           LAG(order_date) OVER (PARTITION BY user_id ORDER BY order_date) AS prev_dt
    FROM user_days
)
SELECT user_id,
       prev_dt,
       order_date                AS curr_dt,
       DATEDIFF(order_date, prev_dt) AS gap_days
FROM lagged
WHERE prev_dt IS NOT NULL
ORDER BY gap_days DESC
LIMIT 12;
