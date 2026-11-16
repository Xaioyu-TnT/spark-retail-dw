-- Q3 行转列：把品类变成列，每个日期一行
-- 考点：CASE WHEN + 聚合（不用 PIVOT 也能实现）
SELECT order_date,
       ROUND(SUM(CASE WHEN category = '电脑' THEN amount ELSE 0 END), 2) AS cat_pc,
       ROUND(SUM(CASE WHEN category = '手机' THEN amount ELSE 0 END), 2) AS cat_phone,
       ROUND(SUM(CASE WHEN category = '家电' THEN amount ELSE 0 END), 2) AS cat_appliance,
       ROUND(SUM(amount), 2)                                              AS total_gmv
FROM dwd_order_detail
GROUP BY order_date
ORDER BY order_date
LIMIT 10;
