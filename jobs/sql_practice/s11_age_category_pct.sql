-- 题11 各年龄段里，各品类 GMV 的占比（行转列 + 占比）
SELECT age_group,
       ROUND(SUM(CASE WHEN category = '电脑' THEN amount ELSE 0 END) / SUM(amount) * 100, 2) AS pc_pct,
       ROUND(SUM(CASE WHEN category = '手机' THEN amount ELSE 0 END) / SUM(amount) * 100, 2) AS phone_pct,
       ROUND(SUM(CASE WHEN category = '图书' THEN amount ELSE 0 END) / SUM(amount) * 100, 2) AS book_pct,
       ROUND(SUM(amount), 2)                                                                 AS gmv
FROM dwd_order_detail
GROUP BY age_group
ORDER BY age_group;
