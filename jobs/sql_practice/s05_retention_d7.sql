-- 题5 7 日留存率（首单后第 7 天仍下单）
WITH first_order AS (
    SELECT user_id, MIN(order_date) AS first_dt
    FROM dwd_order_detail
    GROUP BY user_id
)
SELECT f.first_dt                                                        AS dt,
       COUNT(DISTINCT f.user_id)                                         AS new_users,
       COUNT(DISTINCT CASE WHEN d.order_date = date_add(f.first_dt, 7)
                           THEN d.user_id END)                           AS retained_d7,
       ROUND(COUNT(DISTINCT CASE WHEN d.order_date = date_add(f.first_dt, 7)
                                 THEN d.user_id END)
             / COUNT(DISTINCT f.user_id) * 100, 2)                       AS d7_pct
FROM first_order f
LEFT JOIN dwd_order_detail d ON d.user_id = f.user_id
GROUP BY f.first_dt
ORDER BY dt
LIMIT 12;
