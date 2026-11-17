-- 题9 每日 GMV 的 3 日移动平均（ROWS BETWEEN）
WITH daily AS (
    SELECT order_date, SUM(amount) AS gmv
    FROM dwd_order_detail
    GROUP BY order_date
)
SELECT order_date,
       ROUND(gmv, 2)                                                          AS gmv,
       ROUND(AVG(gmv) OVER (ORDER BY order_date ROWS BETWEEN 2 PRECEDING
                            AND CURRENT ROW), 2)                              AS ma3
FROM daily
ORDER BY order_date
LIMIT 12;
