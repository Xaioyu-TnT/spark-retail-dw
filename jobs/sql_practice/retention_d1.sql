-- 次日留存率：按首次下单日分组，统计次日是否再次下单
-- 考点：CTE + LEFT JOIN + 日期函数 + 比率计算
WITH first_day AS (
    SELECT user_id, MIN(order_date) AS first_dt
    FROM dwd_order_detail
    GROUP BY user_id
)
SELECT f.first_dt                                                        AS dt,
       COUNT(DISTINCT f.user_id)                                         AS new_users,
       COUNT(DISTINCT d.user_id)                                         AS retained_d1,
       ROUND(COUNT(DISTINCT d.user_id) / COUNT(DISTINCT f.user_id) * 100, 2) AS retention_d1
FROM first_day f
LEFT JOIN dwd_order_detail d
       ON d.user_id = f.user_id
      AND d.order_date = date_add(f.first_dt, 1)
GROUP BY f.first_dt
ORDER BY f.first_dt;
