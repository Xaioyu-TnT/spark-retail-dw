-- 题14 RFM 的 R：每个用户最后一次下单距今天数
WITH last_order AS (
    SELECT user_id, MAX(order_date) AS last_dt
    FROM dwd_order_detail
    GROUP BY user_id
)
SELECT user_id,
       last_dt,
       DATEDIFF(DATE '2026-03-31', last_dt) AS recency_days
FROM last_order
ORDER BY recency_days DESC
LIMIT 10;
