-- Q7 每个用户最新一笔订单
-- 考点：ROW_NUMBER + 过滤 rn=1（比 MAX+JOIN 更简洁）
WITH ranked AS (
    SELECT user_id, order_id, order_date, amount,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date DESC, order_id DESC) AS rn
    FROM dwd_order_detail
)
SELECT user_id, order_id, order_date, amount
FROM ranked
WHERE rn = 1
ORDER BY order_date DESC
LIMIT 10;
