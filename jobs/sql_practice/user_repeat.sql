-- 复购率：下单次数 >= 2 的用户占比
-- 考点：子查询 + CASE WHEN 聚合
WITH per_user AS (
    SELECT user_id, COUNT(DISTINCT order_id) AS order_cnt
    FROM dwd_order_detail
    GROUP BY user_id
)
SELECT COUNT(*)                                                          AS all_users,
       SUM(CASE WHEN order_cnt >= 2 THEN 1 ELSE 0 END)                   AS repeat_users,
       ROUND(SUM(CASE WHEN order_cnt >= 2 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS repeat_rate
FROM per_user;
