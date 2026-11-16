-- Q4 列转行：把三个指标列拆成三行
-- 考点：UNION ALL（注意列类型要对齐）
SELECT order_date, 'order_cnt' AS metric, CAST(order_cnt AS DOUBLE) AS value
FROM ads_daily_gmv
UNION ALL
SELECT order_date, 'uv', CAST(uv AS DOUBLE) FROM ads_daily_gmv
UNION ALL
SELECT order_date, 'gmv', CAST(gmv AS DOUBLE) FROM ads_daily_gmv
ORDER BY order_date, metric
LIMIT 12;
