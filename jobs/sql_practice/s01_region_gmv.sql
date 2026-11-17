-- 题1 每个地区的订单数与 GMV（基础聚合）
SELECT region_name,
       COUNT(DISTINCT order_id)      AS orders,
       ROUND(SUM(amount), 2)         AS gmv
FROM dwd_order_detail
GROUP BY region_name
ORDER BY gmv DESC;
