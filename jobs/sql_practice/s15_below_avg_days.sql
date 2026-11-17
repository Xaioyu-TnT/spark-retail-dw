-- 题15 找出 GMV 低于全期平均值的日期（CROSS JOIN 求均值后过滤）
WITH daily AS (
    SELECT order_date, SUM(amount) AS gmv
    FROM dwd_order_detail
    GROUP BY order_date
),
avg_gmv AS (
    SELECT AVG(gmv) AS avg_value FROM daily
)
SELECT d.order_date,
       ROUND(d.gmv, 2)            AS gmv,
       ROUND(a.avg_value, 2)      AS avg_gmv
FROM daily d
CROSS JOIN avg_gmv a
WHERE d.gmv < a.avg_value
ORDER BY d.order_date
LIMIT 12;
