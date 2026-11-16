-- Q5 每天的累计 GMV
-- 考点：SUM() OVER (ORDER BY ...) 累计窗口
SELECT order_date,
       ROUND(SUM(gmv), 2)                                    AS daily_gmv,
       ROUND(SUM(SUM(gmv)) OVER (ORDER BY order_date), 2)     AS cumulative_gmv
FROM ads_daily_gmv
GROUP BY order_date
ORDER BY order_date
LIMIT 10;
