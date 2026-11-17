-- 题4 每日新用户数（首单日 = 新用户日）
WITH first_order AS (
    SELECT user_id, MIN(order_date) AS first_dt
    FROM dwd_order_detail
    GROUP BY user_id
)
SELECT first_dt AS dt, COUNT(*) AS new_users
FROM first_order
GROUP BY first_dt
ORDER BY dt
LIMIT 12;
