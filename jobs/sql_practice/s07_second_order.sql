-- 题7 每个用户的第二笔订单（ROW_NUMBER + 过滤 rn=2）
WITH ranked AS (
    SELECT user_id, order_id, order_date, amount,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date, order_id) AS rn
    FROM dwd_order_detail
)
SELECT user_id, order_id, order_date, amount
FROM ranked
WHERE rn = 2
ORDER BY order_date
LIMIT 10;
