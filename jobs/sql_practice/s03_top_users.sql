-- 题3 下单金额最高的前 10 个用户（排序 + LIMIT）
SELECT user_id,
       COUNT(DISTINCT order_id)   AS orders,
       ROUND(SUM(amount), 2)      AS gmv
FROM dwd_order_detail
GROUP BY user_id
ORDER BY gmv DESC
LIMIT 10;
