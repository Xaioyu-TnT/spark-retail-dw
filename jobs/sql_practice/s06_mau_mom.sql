-- 题6 月度活跃用户（MAU）与环比
WITH m AS (
    SELECT date_format(order_date, 'yyyy-MM') AS ym,
           COUNT(DISTINCT user_id)            AS mau
    FROM dwd_order_detail
    GROUP BY date_format(order_date, 'yyyy-MM')
),
lagged AS (
    SELECT ym, mau, LAG(mau) OVER (ORDER BY ym) AS prev_mau FROM m
)
SELECT ym, mau, prev_mau,
       ROUND((mau - prev_mau) / prev_mau * 100, 2) AS mom_pct
FROM lagged
ORDER BY ym;
