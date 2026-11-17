-- 题2 每个品类的客单价 = GMV / 订单数（注意订单数要去重）
SELECT category,
       COUNT(DISTINCT order_id)                                AS orders,
       ROUND(SUM(amount), 2)                                   AS gmv,
       ROUND(SUM(amount) / COUNT(DISTINCT order_id), 2)        AS aov
FROM dwd_order_detail
GROUP BY category
ORDER BY aov DESC;
