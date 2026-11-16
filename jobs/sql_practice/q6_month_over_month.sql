-- Q6 月度环比（本月 vs 上月）
-- 考点：LAG() OVER 取上一行 + 环比计算
WITH m AS (
    SELECT date_format(order_date, 'yyyy-MM') AS ym,
           ROUND(SUM(gmv), 2)                 AS gmv
    FROM ads_daily_gmv
    GROUP BY date_format(order_date, 'yyyy-MM')
),
lagged AS (
    SELECT ym, gmv,
           LAG(gmv) OVER (ORDER BY ym) AS prev_gmv
    FROM m
)
SELECT ym, gmv, prev_gmv,
       ROUND((gmv - prev_gmv) / prev_gmv * 100, 2) AS mom_pct
FROM lagged
ORDER BY ym;
