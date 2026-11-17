-- 题10 每月各品类 GMV 一行（行转列，CASE WHEN + 聚合）
SELECT date_format(order_date, 'yyyy-MM')                                       AS ym,
       ROUND(SUM(CASE WHEN category = '电脑' THEN amount ELSE 0 END), 2)        AS cat_pc,
       ROUND(SUM(CASE WHEN category = '手机' THEN amount ELSE 0 END), 2)        AS cat_phone,
       ROUND(SUM(CASE WHEN category = '家电' THEN amount ELSE 0 END), 2)        AS cat_appliance,
       ROUND(SUM(amount), 2)                                                    AS total_gmv
FROM dwd_order_detail
GROUP BY date_format(order_date, 'yyyy-MM')
ORDER BY ym;
